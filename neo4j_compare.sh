#!/usr/bin/env bash
# neo4j_compare.sh - compare one orgId + version between two Neo4j databases (uses cypher-shell)
#
# Compares: total objects / nodes / relationships, per-label and per-type counts, label sets,
# relationship patterns, cross-scope relationships, property keys + value types, orphan nodes,
# degree stats, key (mRID) integrity, indexes, constraints, schema tokens, server/APOC versions.
# --deep adds a node-by-node and relationship-by-relationship diff on the key property.
#
# Needs: bash 4+, cypher-shell (Neo4j 5.x; 5.13+ for property value types), awk, sed, sort.
#
# Usage:
#   ./neo4j_compare.sh --src-uri bolt://prod:7687 --tgt-uri bolt://qa:7687 --org ORG01 --version 42
#   ./neo4j_compare.sh --src-uri bolt://localhost:7687 --org ORG01 --src-version 41 --tgt-version 42 --deep
#
# Options (every target value defaults to the source value):
#   --src-uri URI --src-user U --src-password P --src-db DB --src-org O --src-version V
#   --tgt-uri URI --tgt-user U --tgt-password P --tgt-db DB --tgt-org O --tgt-version V
#   --org O --version V          same org/version on both sides
#   --org-prop NAME              default orgId
#   --version-prop NAME          default version
#   --key-prop NAME              default mRID ("" disables key checks and --deep)
#   --ignore-props a,b           extra properties ignored by --deep (org/version always ignored)
#   --deep                       node/relationship-level diff (exports every node: slow on big graphs)
#   --only p1,p2 / --skip p1,p2  run / skip checks whose name starts with these prefixes
#   --list-checks                show the checks and exit
#   --max-print N                diff rows printed per check (default 25)
#   --timeout SECS               limit per side per check (uses coreutils `timeout`)
#   --out-dir DIR                default ./neo4j_compare_<timestamp>
#   Passwords: --src/--tgt-password, env NEO4J_SRC_PASSWORD / NEO4J_TGT_PASSWORD, else prompted.
#   Set CYPHER_SHELL=/path/to/cypher-shell if it is not on PATH.
#
# Output: summary on screen, <out-dir>/diffs.tsv (every difference), <out-dir>/summary.tsv,
#         raw per-side results in <out-dir>/src and <out-dir>/tgt, errors in <out-dir>/errors.log
# Exit:   0 = no data/schema differences, 1 = differences found, 2 = a check failed

set -uo pipefail
shopt -u patsub_replacement 2>/dev/null || true

usage() { awk 'NR>1 && /^#/{sub(/^# ?/,""); print; next} NR>1{exit}' "$0"; exit "${1:-0}"; }

# ------------------------------------------------------------------ arguments
SRC_URI=""; SRC_USER="neo4j"; SRC_PASS="${NEO4J_SRC_PASSWORD:-}"; SRC_DB=""; SRC_ORG=""; SRC_VER=""
TGT_URI=""; TGT_USER="";      TGT_PASS="${NEO4J_TGT_PASSWORD:-}"; TGT_DB=""; TGT_ORG=""; TGT_VER=""
ORG=""; VER=""; ORG_PROP="orgId"; VER_PROP="version"; KEY_PROP="mRID"; IGNORE_PROPS=""
DEEP=0; ONLY=""; SKIP=""; LIST=0; MAX_PRINT=25; TIMEOUT=""; OUT=""
CYPHER_SHELL="${CYPHER_SHELL:-cypher-shell}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --src-uri) SRC_URI=$2; shift 2 ;;      --tgt-uri) TGT_URI=$2; shift 2 ;;
    --src-user) SRC_USER=$2; shift 2 ;;    --tgt-user) TGT_USER=$2; shift 2 ;;
    --src-password) SRC_PASS=$2; shift 2 ;; --tgt-password) TGT_PASS=$2; shift 2 ;;
    --src-db) SRC_DB=$2; shift 2 ;;        --tgt-db) TGT_DB=$2; shift 2 ;;
    --src-org) SRC_ORG=$2; shift 2 ;;      --tgt-org) TGT_ORG=$2; shift 2 ;;
    --src-version) SRC_VER=$2; shift 2 ;;  --tgt-version) TGT_VER=$2; shift 2 ;;
    --org) ORG=$2; shift 2 ;;              --version) VER=$2; shift 2 ;;
    --org-prop) ORG_PROP=$2; shift 2 ;;    --version-prop) VER_PROP=$2; shift 2 ;;
    --key-prop) KEY_PROP=$2; shift 2 ;;    --ignore-props) IGNORE_PROPS=$2; shift 2 ;;
    --deep) DEEP=1; shift ;;               --list-checks) LIST=1; shift ;;
    --only) ONLY=$2; shift 2 ;;            --skip) SKIP=$2; shift 2 ;;
    --max-print) MAX_PRINT=$2; shift 2 ;;  --timeout) TIMEOUT=$2; shift 2 ;;
    --out-dir) OUT=$2; shift 2 ;;
    -h|--help) usage 0 ;;
    *) echo "Unknown option: $1" >&2; usage 2 ;;
  esac
done

# ------------------------------------------------------------------ Cypher helpers
BT='`'
ident() { local s=${1//$BT/$BT$BT}; printf '%s' "$BT$s$BT"; }          # `name`
cy_str() { local s=${1//\\/\\\\}; s=${s//\'/\\\'}; printf "'%s'" "$s"; } # 'text'
cy_list() {   # match the value whether stored as string or number: ['42', 42]
  local v=$1
  if [[ $v =~ ^-?[0-9]+(\.[0-9]+)?$ ]]; then
    printf '[%s, %s]' "$(cy_str "$v")" "$(awk -v x="$v" 'BEGIN{print x+0}')"
  else
    printf '[%s]' "$(cy_str "$v")"
  fi
}

OP=$(ident "$ORG_PROP"); VP=$(ident "$VER_PROP"); KP=""
[[ -n $KEY_PROP ]] && KP=$(ident "$KEY_PROP")

IGNORE_LIST="$(cy_str "$ORG_PROP"), $(cy_str "$VER_PROP")"
IFS=',' read -ra _extra <<< "$IGNORE_PROPS"
for p in "${_extra[@]}"; do p=$(echo "$p" | xargs); [[ -n $p ]] && IGNORE_LIST+=", $(cy_str "$p")"; done
IGNORE_LIST="[$IGNORE_LIST]"

# __ORGS__ / __VERS__ are filled in per side, so each side can use its own org/version
sc()  { echo "$1.$OP IN __ORGS__ AND $1.$VP IN __VERS__"; }
nsc() { echo "NOT coalesce($1.$OP IN __ORGS__ AND $1.$VP IN __VERS__, false)"; }
jn()  { echo "coalesce(reduce(s = '', x IN $1 | s + CASE WHEN s = '' THEN '' ELSE '$2' END + x), '$3')"; }
lbls() { echo "reduce(s = '', x IN COLLECT { UNWIND labels($1) AS lbl WITH lbl ORDER BY lbl RETURN lbl } | s + CASE WHEN s = '' THEN '' ELSE ':' END + x)"; }
INT="(a)-[r]->(b) WHERE $(sc a) AND $(sc b)"

# ------------------------------------------------------------------ checks
# Every query returns two columns, k and v.  kind: info (never fails) | schema | data | deep
declare -A Q FB KIND DESC
ORDER=()
add() { ORDER+=("$1"); KIND[$1]=$2; DESC[$1]=$3; Q[$1]=$4; FB[$1]=${5:-}; }

add server.info info "Neo4j version and edition" \
  "CALL dbms.components() YIELD name, versions, edition RETURN name AS k, versions[0] + ' (' + edition + ')' AS v;"
add server.apoc info "APOC version" \
  "RETURN 'apoc' AS k, apoc.version() AS v;" \
  "RETURN 'apoc' AS k, 'not installed' AS v;"
add db.totals info "Whole-database node/relationship counts (all orgs)" \
  "MATCH (n) RETURN 'all_nodes' AS k, count(n) AS v;
   MATCH ()-[r]->() RETURN 'all_relationships' AS k, count(r) AS v;"
add db.versions_for_org info "Versions present for this orgId, with node counts" \
  "MATCH (n) WHERE n.$OP IN __ORGS__ RETURN 'version=' + coalesce(toString(n.$VP), 'null') AS k, count(*) AS v;"
add schema.tokens schema "Labels, relationship types, property keys known to the DB" \
  "CALL db.labels() YIELD label RETURN 'label:' + label AS k, 'present' AS v;
   CALL db.relationshipTypes() YIELD relationshipType RETURN 'relType:' + relationshipType AS k, 'present' AS v;
   CALL db.propertyKeys() YIELD propertyKey RETURN 'propKey:' + propertyKey AS k, 'present' AS v;"
add schema.indexes schema "Indexes (matched by definition, not name) and their state" \
  "SHOW INDEXES YIELD type, entityType, labelsOrTypes, properties, state
   RETURN type + ' ' + entityType + ' ' + $(jn labelsOrTypes ':' '*') + '(' + $(jn properties ',' '') + ')' AS k, state AS v;"
add schema.constraints schema "Constraints (matched by definition)" \
  "SHOW CONSTRAINTS YIELD type, entityType, labelsOrTypes, properties
   RETURN type + ' ' + entityType + ' ' + $(jn labelsOrTypes ':' '*') + '(' + $(jn properties ',' '') + ')' AS k, 'present' AS v;"
add scope.totals data "Total objects, nodes, relationships for orgId+version" \
  "MATCH (n) WHERE $(sc n) WITH count(n) AS nodes
   CALL { MATCH $INT RETURN count(r) AS internal, count(DISTINCT type(r)) AS types }
   CALL { MATCH (a)-[r]->(b) WHERE $(sc a) AND $(nsc b) RETURN count(r) AS outx }
   CALL { MATCH (a)-[r]->(b) WHERE $(sc b) AND $(nsc a) RETURN count(r) AS inx }
   CALL { MATCH (m) WHERE $(sc m) UNWIND labels(m) AS l RETURN count(DISTINCT l) AS labelCount }
   UNWIND [['total_objects (nodes+internal rels)', nodes + internal], ['nodes', nodes],
           ['relationships_internal', internal], ['relationships_outgoing_to_other_scope', outx],
           ['relationships_incoming_from_other_scope', inx], ['distinct_labels', labelCount],
           ['distinct_relationship_types', types]] AS kv
   RETURN kv[0] AS k, kv[1] AS v;"
add scope.labels data "Node count per label" \
  "MATCH (n) WHERE $(sc n) UNWIND labels(n) AS l RETURN l AS k, count(*) AS v;"
add scope.label_combinations data "Node count per exact label set" \
  "MATCH (n) WHERE $(sc n) RETURN $(lbls n) AS k, count(*) AS v;"
add scope.relationship_types data "Internal relationship count per type" \
  "MATCH $INT RETURN type(r) AS k, count(*) AS v;"
add scope.relationship_patterns data "(labels)-[TYPE]->(labels) counts" \
  "MATCH $INT RETURN '(' + $(lbls a) + ')-[:' + type(r) + ']->(' + $(lbls b) + ')' AS k, count(*) AS v;"
add scope.cross_scope_relationships data "Relationships leaving/entering the scope, per type" \
  "MATCH (a)-[r]->(b) WHERE $(sc a) AND $(nsc b) RETURN 'out:' + type(r) AS k, count(*) AS v;
   MATCH (a)-[r]->(b) WHERE $(sc b) AND $(nsc a) RETURN 'in:' + type(r) AS k, count(*) AS v;"
add scope.node_properties data "Per label: property key, value type, how many nodes have it" \
  "MATCH (n) WHERE $(sc n) UNWIND labels(n) AS l UNWIND keys(n) AS p
   RETURN l + '.' + p + ' <' + valueType(n[p]) + '>' AS k, count(*) AS v;" \
  "MATCH (n) WHERE $(sc n) UNWIND labels(n) AS l UNWIND keys(n) AS p RETURN l + '.' + p AS k, count(*) AS v;"
add scope.relationship_properties data "Per rel type: property key, value type, count" \
  "MATCH $INT UNWIND keys(r) AS p RETURN type(r) + '.' + p + ' <' + valueType(r[p]) + '>' AS k, count(*) AS v;" \
  "MATCH $INT UNWIND keys(r) AS p RETURN type(r) + '.' + p AS k, count(*) AS v;"
add scope.orphans data "Nodes with no relationships, per label" \
  "MATCH (n) WHERE $(sc n) AND NOT EXISTS { (n)--() } UNWIND labels(n) AS l RETURN l AS k, count(*) AS v;"
add scope.degree data "Min/max/avg/total degree per label" \
  "MATCH (n) WHERE $(sc n) WITH n, COUNT { (n)--() } AS d UNWIND labels(n) AS l
   WITH l, min(d) AS mn, max(d) AS mx, round(avg(d) * 100) / 100 AS av, sum(d) AS tot
   UNWIND [['min', mn], ['max', mx], ['avg', av], ['total', tot]] AS kv
   RETURN l + '.' + kv[0] AS k, kv[1] AS v;"
if [[ -n $KEY_PROP ]]; then
  KL=${KEY_PROP//\'/}
  add scope.key_integrity data "Per label: nodes missing the key property, duplicate keys" \
    "MATCH (n) WHERE $(sc n) UNWIND labels(n) AS l
     WITH l, count(*) AS total, count(n.$KP) AS has, count(DISTINCT n.$KP) AS dist
     UNWIND [['missing_$KL', total - has], ['duplicate_$KL', has - dist]] AS kv
     RETURN l + '.' + kv[0] AS k, kv[1] AS v;"
  add deep.nodes deep "Every node by key: missing / extra / labels or properties changed" \
    "MATCH (n) WHERE $(sc n) AND n.$KP IS NOT NULL
     RETURN toString(n.$KP) AS k,
            [COLLECT { UNWIND labels(n) AS lbl WITH lbl ORDER BY lbl RETURN lbl },
             COLLECT { UNWIND keys(n) AS p WITH p WHERE NOT p IN __IGNORE__ WITH p ORDER BY p RETURN [p, n[p]] }] AS v;
     MATCH (n) WHERE $(sc n) AND n.$KP IS NULL RETURN '__nodes_without_key__' AS k, count(*) AS v;"
  add deep.relationships deep "Every internal relationship by (startKey)-[TYPE]->(endKey)" \
    "MATCH $INT AND a.$KP IS NOT NULL AND b.$KP IS NOT NULL
     RETURN '(' + toString(a.$KP) + ')-[:' + type(r) + ']->(' + toString(b.$KP) + ')' AS k,
            COLLECT { UNWIND keys(r) AS p WITH p WHERE NOT p IN __IGNORE__ WITH p ORDER BY p RETURN [p, r[p]] } AS v;
     MATCH $INT AND (a.$KP IS NULL OR b.$KP IS NULL) RETURN '__rels_with_keyless_endpoint__' AS k, count(*) AS v;"
fi

if (( LIST )); then
  for n in "${ORDER[@]}"; do printf '%-36s %-7s %s\n' "$n" "${KIND[$n]}" "${DESC[$n]}"; done
  exit 0
fi

# ------------------------------------------------------------------ validate + resolve
[[ -z $SRC_URI ]] && { echo "--src-uri is required" >&2; usage 2; }
command -v "$CYPHER_SHELL" >/dev/null || { echo "cypher-shell not found (set CYPHER_SHELL=...)" >&2; exit 2; }
SRC_ORG=${SRC_ORG:-$ORG}; SRC_VER=${SRC_VER:-$VER}
[[ -z $SRC_ORG || -z $SRC_VER ]] && { echo "Give --org/--version (or --src-org/--src-version)" >&2; exit 2; }
TGT_ORG=${TGT_ORG:-$SRC_ORG}; TGT_VER=${TGT_VER:-$SRC_VER}
TGT_URI=${TGT_URI:-$SRC_URI}; TGT_USER=${TGT_USER:-$SRC_USER}; TGT_DB=${TGT_DB:-$SRC_DB}
if [[ "$SRC_URI|$SRC_DB|$SRC_ORG|$SRC_VER" == "$TGT_URI|$TGT_DB|$TGT_ORG|$TGT_VER" ]]; then
  echo "Source and target are the same scope - change URI, DB, org or version." >&2; exit 2
fi
(( DEEP )) && [[ -z $KEY_PROP ]] && { echo "--deep needs --key-prop" >&2; exit 2; }

[[ -z $SRC_PASS ]] && { read -rsp "Source password: " SRC_PASS; echo; }
if [[ -z $TGT_PASS ]]; then
  if [[ $TGT_URI == "$SRC_URI" && $TGT_USER == "$SRC_USER" ]]; then TGT_PASS=$SRC_PASS
  else read -rsp "Target password: " TGT_PASS; echo; fi
fi

SRC_ORGS=$(cy_list "$SRC_ORG"); SRC_VERS=$(cy_list "$SRC_VER")
TGT_ORGS=$(cy_list "$TGT_ORG"); TGT_VERS=$(cy_list "$TGT_VER")
TO=(); [[ -n $TIMEOUT ]] && TO=(timeout "$TIMEOUT")

OUT=${OUT:-neo4j_compare_$(date +%Y%m%d_%H%M%S)}
mkdir -p "$OUT/src" "$OUT/tgt"
: > "$OUT/errors.log"
printf 'check\tkind\tkey\tsource\ttarget\tdelta\n' > "$OUT/diffs.tsv"
printf 'check\tkind\tstatus\tsrc_entries\ttgt_entries\tdiffs\tseconds\n' > "$OUT/summary.tsv"

# ------------------------------------------------------------------ execution
exec_q() {   # side query -> raw cypher-shell output
  local side=$1 q=$2 uri user pass db orgs vers
  if [[ $side == src ]]; then uri=$SRC_URI user=$SRC_USER pass=$SRC_PASS db=$SRC_DB orgs=$SRC_ORGS vers=$SRC_VERS
  else                        uri=$TGT_URI user=$TGT_USER pass=$TGT_PASS db=$TGT_DB orgs=$TGT_ORGS vers=$TGT_VERS; fi
  q=${q//__ORGS__/$orgs}; q=${q//__VERS__/$vers}; q=${q//__IGNORE__/$IGNORE_LIST}
  local args=(-a "$uri" -u "$user" --format plain)
  [[ -n $db ]] && args+=(-d "$db")
  printf '%s\n' "$q" | NEO4J_PASSWORD="$pass" ${TO[@]+"${TO[@]}"} "$CYPHER_SHELL" "${args[@]}"
}

parse() {    # plain output -> "key|value" lines
  sed -E '/^k, v$/d; s/^"((\\.|[^"\\])*)", /\1|/; s/^([^|"]*), /\1|/; s/\|"(.*)"$/|\1/; s/\\"/"/g'
}

run_side() { # side check -> 0 ok / 1 failed
  local side=$1 name=$2
  local base="$OUT/$side/$name"
  if exec_q "$side" "${Q[$name]}" > "$base.raw" 2> "$base.err"; then :
  elif [[ -n ${FB[$name]} ]] && exec_q "$side" "${FB[$name]}" > "$base.raw" 2>> "$base.err"; then :
  else return 1; fi
  parse < "$base.raw" | LC_ALL=C sort > "$base.txt"
}

compare() {  # check kind fileA fileB -> TSV diff rows
  LC_ALL=C awk -v chk="$1" -v kind="$2" '
    function key(s,  i) { i = index(s, "|"); return i ? substr(s, 1, i - 1) : s }
    function val(s,  i) { i = index(s, "|"); return i ? substr(s, i + 1) : "" }
    FILENAME == ARGV[1] { x = key($0); if (x in A) A[x] = A[x] ";" val($0); else A[x] = val($0); K[x] = 1; next }
                        { x = key($0); if (x in B) B[x] = B[x] ";" val($0); else B[x] = val($0); K[x] = 1 }
    END {
      num = "^-?[0-9]+(\\.[0-9]+)?$"
      for (x in K) {
        sa = (x in A) ? A[x] : "<missing>"; sb = (x in B) ? B[x] : "<missing>"
        if (sa == sb) continue
        if (sa ~ num && sb ~ num) d = sb - sa
        else if (!(x in A)) d = "only in target"
        else if (!(x in B)) d = "only in source"
        else d = "changed"
        print chk "\t" kind "\t" x "\t" sa "\t" sb "\t" d
      }
    }' "$3" "$4" | LC_ALL=C sort -t $'\t' -k3,3
}

selected() {
  local n=$1 p
  [[ ${KIND[$n]} == deep && $DEEP -eq 0 ]] && return 1
  if [[ -n $ONLY ]]; then
    local hit=1; IFS=',' read -ra ps <<< "$ONLY"
    for p in "${ps[@]}"; do [[ $n == "$p"* ]] && hit=0; done
    (( hit == 0 )) || return 1
  fi
  if [[ -n $SKIP ]]; then
    IFS=',' read -ra ps <<< "$SKIP"
    for p in "${ps[@]}"; do [[ $n == "$p"* ]] && return 1; done
  fi
  return 0
}

echo "SOURCE  $SRC_URI db=${SRC_DB:-default} org=$SRC_ORG version=$SRC_VER"
echo "TARGET  $TGT_URI db=${TGT_DB:-default} org=$TGT_ORG version=$TGT_VER"
echo

for name in "${ORDER[@]}"; do
  selected "$name" || continue
  kind=${KIND[$name]}; t0=$SECONDS
  printf '  %-36s' "$name"
  run_side src "$name" & p1=$!
  run_side tgt "$name" & p2=$!
  wait $p1; r1=$?; wait $p2; r2=$?
  if (( r1 || r2 )); then
    st=ERROR; ns=-; nt=-; nd=0
    { echo "== $name"
      (( r1 )) && sed 's/^/  [source] /' "$OUT/src/$name.err"
      (( r2 )) && sed 's/^/  [target] /' "$OUT/tgt/$name.err"; } >> "$OUT/errors.log"
  else
    compare "$name" "$kind" "$OUT/src/$name.txt" "$OUT/tgt/$name.txt" > "$OUT/$name.diff"
    nd=$(( $(wc -l < "$OUT/$name.diff") ))
    ns=$(( $(wc -l < "$OUT/src/$name.txt") )); nt=$(( $(wc -l < "$OUT/tgt/$name.txt") ))
    cat "$OUT/$name.diff" >> "$OUT/diffs.tsv"
    if (( nd == 0 )); then st=MATCH; elif [[ $kind == info ]]; then st=INFO; else st=DIFF; fi
  fi
  secs=$(( SECONDS - t0 ))
  echo "$st (${secs}s)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$kind" "$st" "$ns" "$nt" "$nd" "$secs" >> "$OUT/summary.tsv"
done

# ------------------------------------------------------------------ report
echo
printf '%0.s=' {1..92}; echo
printf '%-36s%-8s%10s%10s%10s%10s\n' CHECK STATUS SRC TGT DIFFS SECS
printf '%0.s-' {1..92}; echo
tail -n +2 "$OUT/summary.tsv" | while IFS=$'\t' read -r n k s a b d t; do
  printf '%-36s%-8s%10s%10s%10s%10s\n' "$n" "$s" "$a" "$b" "$d" "$t"
done
printf '%0.s=' {1..92}; echo

tail -n +2 "$OUT/summary.tsv" | while IFS=$'\t' read -r n k s a b d t; do
  [[ $s == ERROR ]] && { echo; echo "[ERROR] $n - see errors.log:"; awk -v h="== $n" '$0==h{f=1;next} /^== /{f=0} f' "$OUT/errors.log" | head -4; }
  [[ $s == DIFF || $s == INFO ]] || continue
  echo; echo "[$s] $n - ${DESC[$n]}"
  if [[ $k == deep ]]; then
    awk -F'\t' '{c[$6]++} END{printf "   only in source: %d   only in target: %d   changed: %d\n",
                 c["only in source"], c["only in target"], c["changed"]}' "$OUT/$n.diff"
  fi
  awk -F'\t' -v max="$MAX_PRINT" '
    function cut(s, w) { return length(s) > w ? substr(s, 1, w - 3) "..." : s }
    NR == 1 { printf "   %-56s %22s %22s  %s\n", "KEY", "SOURCE", "TARGET", "DELTA" }
    NR <= max { printf "   %-56s %22s %22s  %s\n", cut($3, 56), cut($4, 22), cut($5, 22), $6 }
    END { if (NR > max) printf "   ... %d more in diffs.tsv\n", NR - max }' "$OUT/$n.diff"
done

echo
echo "Reports: $OUT/summary.tsv  $OUT/diffs.tsv  (errors: $OUT/errors.log)"

grep -q $'\tERROR\t' "$OUT/summary.tsv" && exit 2
grep -q $'\tDIFF\t' "$OUT/summary.tsv" && exit 1
exit 0
