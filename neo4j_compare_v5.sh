#!/usr/bin/env bash
# neo4j_compare_csv.sh - compare one orgId (and one or more versions) between two Neo4j databases
#                        and write the results as CSV files.
#
#   nodes.csv              node count per label set, source vs target, difference, status, comment
#   relationships.csv      relationship count per (labels)-[:TYPE]->(labels), src vs tgt, comment
#   properties.csv         per label / relationship type: property, data type, how many have it, comment
#   property_values.csv    (COMPARE_VALUES=1) every property value by mRID that differs
#   relationship_details.csv (COMPARE_VALUES=1) every relationship by (mRID)-[:TYPE]->(mRID) that differs
#   summary.csv            one line per version per check
#
# Fill in the CONFIG block and run:   ./neo4j_compare_csv.sh
# Optional overrides:  --org CAISO  --versions DB137-1,DB137-2  --values  --out-dir DIR
# Needs: bash 4+, cypher-shell (Neo4j 5.x), awk, sed, sort, join.
# Exit:  0 = everything matches, 1 = differences found, 2 = a query failed

set -uo pipefail
shopt -u patsub_replacement 2>/dev/null || true

# ==================================================================================
#  CONFIG
# ==================================================================================
SRC_NAME="DEV"                      # used as the column heading in the CSVs
SRC_URI="bolt://dev-server:7687"
SRC_USER="neo4j"
SRC_PASS=""                         # empty -> env NEO4J_SRC_PASSWORD, else prompted
SRC_DB=""                           # empty -> default database

TGT_NAME="INT"
TGT_URI="bolt://int-server:7687"
TGT_USER="neo4j"
TGT_PASS=""                         # empty -> env NEO4J_TGT_PASSWORD, else prompted
TGT_DB=""

ORG_ID="CAISO"
VERSIONS=("DB137-1" "DB137-2")      # same version name on both sides.
                                    # To compare different names use "SRC=TGT", e.g. "DB137-1=DB137-2"

ORG_PROP="orgId"
VER_PROP="version"
KEY_PROP="mRID"                     # used to match nodes for COMPARE_VALUES

COMPARE_VALUES=0                    # 1 = also compare every property value and every relationship by mRID
IGNORE_PROPS=""                     # e.g. "createdAt,updatedAt" - skipped in the value comparison

OUT_DIR=""                          # empty -> ./neo4j_compare_<timestamp>
TIMEOUT=""                          # seconds per query per side, empty = no limit
CYPHER_SHELL="${CYPHER_SHELL:-cypher-shell}"   # or a full path, e.g. /opt/neo4j/bin/cypher-shell
# ================================================================== end of CONFIG

while [[ $# -gt 0 ]]; do
  case "$1" in
    --org) ORG_ID=$2; shift 2 ;;
    --versions) IFS=',' read -ra VERSIONS <<< "$2"; shift 2 ;;
    --values) COMPARE_VALUES=1; shift ;;
    --out-dir) OUT_DIR=$2; shift 2 ;;
    -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done

command -v "$CYPHER_SHELL" >/dev/null || {
  echo "cypher-shell not found. Set CYPHER_SHELL in the CONFIG block to its full path." >&2; exit 2; }
SRC_PASS=${SRC_PASS:-${NEO4J_SRC_PASSWORD:-}}
TGT_PASS=${TGT_PASS:-${NEO4J_TGT_PASSWORD:-}}
[[ -z $SRC_PASS ]] && { read -rsp "$SRC_NAME password: " SRC_PASS; echo; }
[[ -z $TGT_PASS ]] && { read -rsp "$TGT_NAME password: " TGT_PASS; echo; }

OUT_DIR=${OUT_DIR:-neo4j_compare_$(date +%Y%m%d_%H%M%S)}
mkdir -p "$OUT_DIR/raw"
ERR="$OUT_DIR/errors.log"; : > "$ERR"
TO=(); [[ -n $TIMEOUT ]] && TO=(timeout "$TIMEOUT")
export LC_ALL=C

# ------------------------------------------------------------------ Cypher building blocks
BT='`'
ident()  { local s=${1//$BT/$BT$BT}; printf '%s' "$BT$s$BT"; }
cy_str() { local s=${1//\\/\\\\}; s=${s//\'/\\\'}; printf "'%s'" "$s"; }

IGN="$(cy_str "$ORG_PROP"), $(cy_str "$VER_PROP"), $(cy_str "$KEY_PROP")"
IFS=',' read -ra _x <<< "$IGNORE_PROPS"
for p in "${_x[@]}"; do p=$(echo "$p" | xargs); [[ -n $p ]] && IGN+=", $(cy_str "$p")"; done

lbl() {  # sorted label set of a node, e.g. Breaker:OPEN
  echo "CASE WHEN size(labels($1)) = 0 THEN '<no label>' ELSE reduce(s = '', x IN COLLECT { UNWIND labels($1) AS l WITH l ORDER BY l RETURN l } | s + CASE s WHEN '' THEN '' ELSE ':' END + x) END"
}

fill() {  # replace placeholders in a query template; $2 = Cypher literal for the version
  local q=$1
  q=${q//__OP__/$(ident "$ORG_PROP")}; q=${q//__VP__/$(ident "$VER_PROP")}; q=${q//__KP__/$(ident "$KEY_PROP")}
  q=${q//__ORG__/$(cy_str "$ORG_ID")}; q=${q//__VER__/$2}; q=${q//__IGN__/[$IGN]}
  q=${q//__LBL_N__/$(lbl n)}; q=${q//__LBL_A__/$(lbl a)}; q=${q//__LBL_B__/$(lbl b)}
  printf '%s' "$q"
}

# Every query returns ONE string column called line:  key|value
read -r -d '' Q_NODES <<'EOF'
MATCH (n) WHERE n.__OP__ = __ORG__ AND n.__VP__ = __VER__
WITH __LBL_N__ AS labelSet, count(*) AS c
RETURN labelSet + '|' + toString(c) AS line;
EOF

read -r -d '' Q_RELS <<'EOF'
MATCH (a)-[r]->(b) WHERE a.__OP__ = __ORG__ AND a.__VP__ = __VER__
WITH a, r, b, coalesce(b.__OP__ = __ORG__ AND b.__VP__ = __VER__, false) AS endInScope
WITH __LBL_A__ AS la, type(r) AS t, __LBL_B__ + CASE WHEN endInScope THEN '' ELSE ' {other org/version}' END AS lb,
     count(*) AS c
RETURN '(' + la + ')-[:' + t + ']->(' + lb + ')|' + toString(c) AS line;
EOF

read -r -d '' Q_NODE_PROPS <<'EOF'
MATCH (n) WHERE n.__OP__ = __ORG__ AND n.__VP__ = __VER__
WITH n, __LBL_N__ AS labelSet
UNWIND keys(n) AS p
WITH labelSet, p, replace(valueType(n[p]), ' NOT NULL', '') AS vt, count(*) AS c
RETURN 'NODE|' + labelSet + '|' + p + '|' + vt + '|' + toString(c) AS line;
EOF

read -r -d '' Q_REL_PROPS <<'EOF'
MATCH (a)-[r]->() WHERE a.__OP__ = __ORG__ AND a.__VP__ = __VER__
UNWIND keys(r) AS p
WITH type(r) AS t, p, replace(valueType(r[p]), ' NOT NULL', '') AS vt, count(*) AS c
RETURN 'RELATIONSHIP|' + t + '|' + p + '|' + vt + '|' + toString(c) AS line;
EOF

read -r -d '' Q_VALUES <<'EOF'
MATCH (n) WHERE n.__OP__ = __ORG__ AND n.__VP__ = __VER__ AND n.__KP__ IS NOT NULL
WITH n, toString(n.__KP__) AS id, __LBL_N__ AS labelSet
UNWIND ['<labels>'] + [k IN keys(n) WHERE NOT k IN __IGN__] AS p
WITH id, p, CASE
       WHEN p = '<labels>' THEN labelSet
       WHEN valueType(n[p]) STARTS WITH 'LIST' THEN '[' + reduce(s = '', x IN n[p] | s + CASE s WHEN '' THEN '' ELSE ', ' END + toString(x)) + ']'
       ELSE toString(n[p]) END AS v
RETURN id + '|' + p + '|v:' + replace(replace(replace(coalesce(v, ''), '\n', '\\n'), '\r', '\\r'), '\t', '\\t') AS line;
EOF

read -r -d '' Q_REL_DETAIL <<'EOF'
MATCH (a)-[r]->(b) WHERE a.__OP__ = __ORG__ AND a.__VP__ = __VER__ AND a.__KP__ IS NOT NULL AND b.__KP__ IS NOT NULL
WITH '(' + toString(a.__KP__) + ')-[:' + type(r) + ']->(' + toString(b.__KP__) + ')' AS k, count(*) AS c
RETURN k + '|' + toString(c) AS line;
EOF

# ------------------------------------------------------------------ running queries
run_side() {  # side name query-template version outfile
  local side=$1 name=$2 tpl=$3 ver=$4 out=$5 uri user pass db
  if [[ $side == src ]]; then uri=$SRC_URI user=$SRC_USER pass=$SRC_PASS db=$SRC_DB
  else                        uri=$TGT_URI user=$TGT_USER pass=$TGT_PASS db=$TGT_DB; fi
  local args=(-a "$uri" -u "$user" --format plain); [[ -n $db ]] && args+=(-d "$db")
  fill "$tpl" "$(cy_str "$ver")" | NEO4J_PASSWORD="$pass" ${TO[@]+"${TO[@]}"} "$CYPHER_SHELL" "${args[@]}" \
      > "$out.raw" 2> "$out.err" || return 1
  # plain output: header "line", then one quoted string per row -> unquote + unescape
  awk 'NR == 1 && $0 == "line" { next }
       $0 == "NULL" || $0 == "null" || $0 == "" { next }
       { s = $0
         if (s ~ /^".*"$/) s = substr(s, 2, length(s) - 2)
         gsub(/\\\\/, "\001", s); gsub(/\\"/, "\"", s); gsub(/\001/, "\\\\", s)
         print s }' "$out.raw" > "$out"
}

run_both() {  # name template src-version tgt-version dir -> 0 ok / 1 failed
  local name=$1 tpl=$2 sv=$3 tv=$4 dir=$5 r1 r2
  printf '    %-24s' "$name"
  run_side src "$name" "$tpl" "$sv" "$dir/src_$name" & local p1=$!
  run_side tgt "$name" "$tpl" "$tv" "$dir/tgt_$name" & local p2=$!
  wait $p1; r1=$?; wait $p2; r2=$?
  if (( r1 || r2 )); then
    echo "FAILED"
    { echo "== $name ($sv / $tv)"
      (( r1 )) && sed "s/^/  [$SRC_NAME] /" "$dir/src_$name.err"
      (( r2 )) && sed "s/^/  [$TGT_NAME] /" "$dir/tgt_$name.err"; } >> "$ERR"
    return 1
  fi
  echo "ok"
}

# ------------------------------------------------------------------ comparing
# count files: "key|count" (key = everything before the last |)  ->  key TAB src TAB tgt  (empty = missing)
merge_counts() {
  awk 'function split_last(s) { i = match(s, /\|[^|]*$/); K = substr(s, 1, i - 1); V = substr(s, i + 1) }
       FNR == NR { split_last($0); A[K] = V; seen[K] = 1; next }
                 { split_last($0); B[K] = V; seen[K] = 1 }
       END { for (k in seen) print k "\t" ((k in A) ? A[k] : "") "\t" ((k in B) ? B[k] : "") }' "$1" "$2" | sort -t $'\t' -k1,1
}

# big files: "key|count" or "id|prop|v:value" -> sorted key TAB value, then join both sides, keep differences
to_tsv_last()  { awk '{ i = match($0, /\|[^|]*$/); print substr($0, 1, i - 1) "\t" substr($0, i + 1) }' "$1" | sort -t $'\t' -k1,1; }
to_tsv_two()   { awk '{ i = index($0, "|"); j = index(substr($0, i + 1), "|")
                        print substr($0, 1, i + j - 1) "\t" substr($0, i + j + 1) }' "$1" | sort -t $'\t' -k1,1; }
join_diff() { join -t $'\t' -a1 -a2 -e '<missing>' -o 0,1.2,2.2 "$1" "$2" | awk -F'\t' '$2 != $3'; }

CSV_AWK='function q(s) { if (s ~ /[",\r\n]/ || s ~ /^[ =+@-]/ && s !~ /^-?[0-9]+$/) { gsub(/"/, "\"\"", s); return "\"" s "\"" } return s }
function status(a, b) { if (a == "") return "ONLY_IN_" T; if (b == "") return "ONLY_IN_" S; return (a == b) ? "MATCH" : "MISMATCH" }'

# ------------------------------------------------------------------ auto-comment rules
# comment_for_nodes(labelSet, src, tgt, status) -> human-readable explanation string
# comment_for_rels(rel, src, tgt, status)       -> human-readable explanation string
# comment_for_props(entity, labelOrType, prop, dataType, src, tgt, status) -> explanation string
#
# Rules applied in order; first match wins.  Empty string = no comment needed.

COMMENT_AWK='
# ---- node comment rules ----
function comment_node(labels, src, tgt, st,    c) {
    c = ""
    if (labels ~ /TIE_LINE/) {
        if (st ~ /^ONLY_IN_/)  return "TIE_LINE label is a DEV-platform feature; not present in legacy INT POC"
        if (st == "MISMATCH")  return "TIE_LINE label grouping splits node counts vs INT where all are plain ACLineSegment"
        return ""
    }
    if (st == "ONLY_IN_" S) return "Label set exists only in " S " — may be a new type added in DEV platform"
    if (st == "ONLY_IN_" T) return "Label set exists only in " T " — may be a legacy-only type not imported in DEV"
    if (st == "MISMATCH") {
        d = src - tgt; if (d < 0) d = -d
        if (d <= 5) return "Count differs by " d " — verify if any source records were filtered or skipped during import"
        return "Count mismatch — check import filter config or source data differences between environments"
    }
    return ""
}

# ---- relationship comment rules ----
function comment_rel(rel, src, tgt, st,    c) {
    if (rel ~ /TIE_LINE/) {
        if (st ~ /^ONLY_IN_/) return "TIE_LINE label on endpoint only in DEV platform; relationship group absent in INT"
        if (st == "MISMATCH") return "TIE_LINE endpoint splits this relationship into a separate row vs INT aggregated count"
        return ""
    }
    # caiso_ (underscore) or caiso. (dot) vendor namespace in relationship key
    if (rel ~ /caiso[_.]/ || rel ~ /spc[_.]/ || rel ~ /rdfs[_.]/) {
        if (st == "MISMATCH" || st ~ /^ONLY_IN_/)
            return "Non-CIM namespace separator changed from colon (caiso:) to underscore (caiso_) in DEV; INT uses legacy colon format"
        return ""
    }
    if (st == "ONLY_IN_" S) return "Relationship type exists only in " S " — may be new relationship added in DEV platform"
    if (st == "ONLY_IN_" T) return "Relationship type exists only in " T " — may be legacy-only relationship not modelled in DEV"
    if (st == "MISMATCH") {
        d = src - tgt; if (d < 0) d = -d
        if (d <= 5) return "Count differs by " d " — verify specific nodes missing a relationship endpoint"
        return "Count mismatch — check if related nodes were filtered or type-excluded during import"
    }
    return ""
}

# ---- property comment rules ----
function comment_prop(entity, labels, prop, dtype, src, tgt, st,    c) {
    # 1. TIE_LINE node group — check first so it overrides everything for non-MATCH rows
    if (labels ~ /TIE_LINE/) {
        if (st != "MATCH") return "TIE_LINE label is DEV-only; property count reflects DEV subset only — not a data error"
        return ""
    }
    # 2. Vendor-namespace property: underscore form (caiso_x) OR dot form (caiso.Class.attr)
    if (prop ~ /^caiso[_.]/ || prop ~ /^spc[_.]/ || prop ~ /^rdfs[_.]/) {
        if (st == "MISMATCH")
            return "Vendor-namespace property: key uses underscore separator in DEV (caiso_x), colon in INT (caiso:x); count difference expected"
        if (st ~ /^ONLY_IN_/)
            return "Vendor-namespace property exists only in one environment due to namespace separator change (caiso: → caiso_)"
        return ""
    }
    # 3. Property present on only one side — often empty-value difference
    if (st ~ /^ONLY_IN_/) {
        side = (st == "ONLY_IN_" S) ? S : T
        return "Property present only in " side " — DEV cim_objects skips blank values; INT POC may have stored empty strings"
    }
    # 4. Count mismatch — one side is 0 → classic blank-value difference
    if (st == "MISMATCH") {
        d = src - tgt; if (d < 0) d = -d
        if (src == 0 || tgt == 0)
            return "One side has 0 occurrences — DEV cim_objects skips blank/empty values; INT POC may have stored them"
        return "Property count differs — check if blank values or type-filter differences exist between environments"
    }
    return ""
}'

header() { [[ -s $1 ]] || echo "$2" > "$1"; }

NODES_CSV="$OUT_DIR/nodes.csv"; RELS_CSV="$OUT_DIR/relationships.csv"; PROPS_CSV="$OUT_DIR/properties.csv"
VALS_CSV="$OUT_DIR/property_values.csv"; RDET_CSV="$OUT_DIR/relationship_details.csv"; SUM_CSV="$OUT_DIR/summary.csv"
header "$NODES_CSV" "Version,Node Labels,$SRC_NAME Count,$TGT_NAME Count,Difference ($SRC_NAME - $TGT_NAME),Status,Comment"
header "$RELS_CSV"  "Version,Relationship,$SRC_NAME Count,$TGT_NAME Count,Difference ($SRC_NAME - $TGT_NAME),Status,Comment"
header "$PROPS_CSV" "Version,Entity,Labels / Type,Property,Data Type,$SRC_NAME Count,$TGT_NAME Count,Difference ($SRC_NAME - $TGT_NAME),Status,Comment"
header "$SUM_CSV"   "Version,Check,$SRC_NAME Total,$TGT_NAME Total,Rows Compared,Rows Different,Result"
if (( COMPARE_VALUES )); then
  header "$VALS_CSV" "Version,$KEY_PROP,Property,$SRC_NAME Value,$TGT_NAME Value,Status"
  header "$RDET_CSV" "Version,Relationship ($KEY_PROP),$SRC_NAME Count,$TGT_NAME Count,Status"
fi

count_report() {  # version merged-file csv kind   (kind: nodes|rels|props) -> appends rows, prints summary line
  local ver=$1 merged=$2 csv=$3 kind=$4
  awk -F'\t' -v V="$ver" -v S="$SRC_NAME" -v T="$TGT_NAME" -v kind="$kind" -v sum="$SUM_CSV" -v csv="$csv" \
      "$CSV_AWK $COMMENT_AWK"'
    { a = $2; b = $3; st = status(a, b); d = (a == "" ? 0 : a) - (b == "" ? 0 : b)
      n++; if (st != "MATCH") bad++
      if (kind == "nodes") {
        cmt = comment_node($1, a+0, b+0, st)
        print q(V) "," q($1) "," a "," b "," d "," st "," q(cmt) >> csv
        ta += a; tb += b
      } else if (kind == "rels") {
        cmt = comment_rel($1, a+0, b+0, st)
        print q(V) "," q($1) "," a "," b "," d "," st "," q(cmt) >> csv
        ta += a; tb += b
      } else if (kind == "props") {
        split($1, f, "|")
        cmt = comment_prop(f[1], f[2], f[3], f[4], a+0, b+0, st)
        print q(V) "," f[1] "," q(f[2]) "," q(f[3]) "," f[4] "," a "," b "," d "," st "," q(cmt) >> csv
      }
    }
    END {
      label = (kind == "nodes") ? "Node counts" : (kind == "rels") ? "Relationship counts" : "Properties"
      if (kind != "props") print q(V) ",TOTAL," ta "," tb "," ta - tb "," ((ta == tb && !bad) ? "MATCH" : "MISMATCH") "," >> csv
      print q(V) "," label "," (kind == "props" ? "" : ta) "," (kind == "props" ? "" : tb) "," n + 0 "," bad + 0 "," (bad ? "MISMATCH" : "MATCH") >> sum
      if (kind == "props") printf "      %-22s %d rows, %d different\n", label, n, bad
      else printf "      %-22s %s=%d  %s=%d  difference=%d  (%d of %d rows different)\n", label, S, ta, T, tb, ta - tb, bad, n
    }' "$merged"
}

# ------------------------------------------------------------------ main loop
echo "$SRC_NAME: $SRC_URI ${SRC_DB:+db=$SRC_DB}"
echo "$TGT_NAME: $TGT_URI ${TGT_DB:+db=$TGT_DB}"
echo "orgId: $ORG_ID"
FAILED=0
idx=0
for entry in "${VERSIONS[@]}"; do
  idx=$((idx + 1))
  if [[ $entry == *=* ]]; then SV=${entry%%=*}; TV=${entry#*=}; else SV=$entry; TV=$entry; fi
  VLABEL=$([[ $SV == "$TV" ]] && echo "$SV" || echo "$SV vs $TV")
  D="$OUT_DIR/raw/v$idx"; mkdir -p "$D"
  echo; echo "== Version $VLABEL"

  if run_both nodes "$Q_NODES" "$SV" "$TV" "$D"; then
    merge_counts "$D/src_nodes" "$D/tgt_nodes" > "$D/nodes.tsv"
  else FAILED=1; fi
  if run_both relationships "$Q_RELS" "$SV" "$TV" "$D"; then
    merge_counts "$D/src_relationships" "$D/tgt_relationships" > "$D/rels.tsv"
  else FAILED=1; fi
  props_ok=1
  run_both node_properties "$Q_NODE_PROPS" "$SV" "$TV" "$D" || { props_ok=0; FAILED=1; }
  run_both rel_properties  "$Q_REL_PROPS"  "$SV" "$TV" "$D" || { props_ok=0; FAILED=1; }
  if (( props_ok )); then
    cat "$D/src_node_properties" "$D/src_rel_properties" > "$D/src_props"
    cat "$D/tgt_node_properties" "$D/tgt_rel_properties" > "$D/tgt_props"
    merge_counts "$D/src_props" "$D/tgt_props" > "$D/props.tsv"
  fi

  vals_ok=0
  if (( COMPARE_VALUES )); then
    vals_ok=1
    if run_both property_values "$Q_VALUES" "$SV" "$TV" "$D"; then
      to_tsv_two "$D/src_property_values" > "$D/src_vals.tsv"; to_tsv_two "$D/tgt_property_values" > "$D/tgt_vals.tsv"
      join_diff "$D/src_vals.tsv" "$D/tgt_vals.tsv" > "$D/vals.diff"
    else vals_ok=0; FAILED=1; fi
    if run_both relationship_details "$Q_REL_DETAIL" "$SV" "$TV" "$D"; then
      to_tsv_last "$D/src_relationship_details" > "$D/src_rdet.tsv"; to_tsv_last "$D/tgt_relationship_details" > "$D/tgt_rdet.tsv"
      join_diff "$D/src_rdet.tsv" "$D/tgt_rdet.tsv" > "$D/rdet.diff"
    else vals_ok=0; FAILED=1; fi
  fi

  echo "    results:"
  [[ -f $D/nodes.tsv ]] && count_report "$VLABEL" "$D/nodes.tsv" "$NODES_CSV" nodes
  [[ -f $D/rels.tsv  ]] && count_report "$VLABEL" "$D/rels.tsv"  "$RELS_CSV"  rels
  [[ -f $D/props.tsv ]] && count_report "$VLABEL" "$D/props.tsv" "$PROPS_CSV" props

  if [[ -f $D/vals.diff ]]; then
    awk -F'\t' -v V="$VLABEL" -v S="$SRC_NAME" -v T="$TGT_NAME" -v csv="$VALS_CSV" "$CSV_AWK"'
      { i = index($1, "|"); id = substr($1, 1, i - 1); p = substr($1, i + 1)
        a = $2; b = $3; sub(/^v:/, "", a); sub(/^v:/, "", b)
        st = ($2 == "<missing>") ? "ONLY_IN_" T : ($3 == "<missing>") ? "ONLY_IN_" S : "MISMATCH"
        if (p == "<labels>") { p = "<node labels>"; if (st != "MISMATCH") st = (st == "ONLY_IN_" S) ? "NODE_ONLY_IN_" S : "NODE_ONLY_IN_" T }
        print q(V) "," q(id) "," q(p) "," q(a) "," q(b) "," st >> csv; n++
        if (st ~ /^NODE_ONLY/) nodes++ }
      END { printf "      %-22s %d differences (%d nodes exist on one side only)\n", "Property values", n, nodes }' "$D/vals.diff"
    nv=$(wc -l < "$D/vals.diff"); tot=$(wc -l < "$D/src_vals.tsv")
    echo "$VLABEL,Property values,$tot,$(wc -l < "$D/tgt_vals.tsv"),,$nv,$([[ $nv -eq 0 ]] && echo MATCH || echo MISMATCH)" >> "$SUM_CSV"
  fi
  if [[ -f $D/rdet.diff ]]; then
    awk -F'\t' -v V="$VLABEL" -v S="$SRC_NAME" -v T="$TGT_NAME" -v csv="$RDET_CSV" "$CSV_AWK"'
      { a = ($2 == "<missing>") ? "" : $2; b = ($3 == "<missing>") ? "" : $3
        print q(V) "," q($1) "," a "," b "," status(a, b) >> csv; n++ }
      END { printf "      %-22s %d differences\n", "Relationships by mRID", n }' "$D/rdet.diff"
    nr=$(wc -l < "$D/rdet.diff")
    echo "$VLABEL,Relationships by $KEY_PROP,$(wc -l < "$D/src_rdet.tsv"),$(wc -l < "$D/tgt_rdet.tsv"),,$nr,$([[ $nr -eq 0 ]] && echo MATCH || echo MISMATCH)" >> "$SUM_CSV"
  fi
done

echo
echo "CSV files in $OUT_DIR/:"
for f in nodes relationships properties property_values relationship_details summary; do
  [[ -f $OUT_DIR/$f.csv ]] && echo "  $f.csv"
done
(( FAILED )) && { echo "Some queries failed - see $ERR"; exit 2; }
grep -q ',MISMATCH$' "$SUM_CSV" && exit 1
exit 0
