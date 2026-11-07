package com.cim.service;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Phase 3 post-import enrichment for cim_objects.
 *
 * ROOT CAUSE FIX — why Map<String,String> failed:
 *   Spring Data MongoDB cannot deserialize Map<String,String> when:
 *   a) Keys contain dots (e.g. "Terminal.ConductingEquipment") — it tries
 *      to interpret them as nested-object path separators → empty map
 *   b) Values are non-String types (e.g. nominalVoltage stored as Double 21.6)
 *      → silently dropped, returns null
 *
 *   FIX: Use raw org.bson.Document for ALL MongoDB reads.
 *   doc.get("resolvedRefIds", Document.class) and doc.get("attributes", Document.class)
 *   return the raw BSON document with dot-containing keys and mixed value types
 *   fully preserved. safeString() converts any value type to String.
 *
 * CONFIRMED KEY NAMES (from your MongoDB data):
 *   resolvedRefIds — raw dots:
 *     "ConductingEquipment.BaseVoltage"              → BaseVoltage rdfId
 *     "Terminal.ConductingEquipment"                 → Equipment rdfId
 *     "Terminal.ConnectivityNode"                    → ConnectivityNode rdfId
 *     "ConnectivityNode.ConnectivityNodeContainer"   → VoltageLevel rdfId
 *     "VoltageLevel.Substation"                      → Substation rdfId
 *     "Substation.Region"                            → SGR rdfId
 *     "SubGeographicalRegion.Region"                 → GR rdfId (some docs use "Substation.Region")
 *   attributes — raw dots (no MapKeySanitizer encoding in your version):
 *     "BaseVoltage.nominalVoltage"  → Double (e.g. 21.6)
 *     "IdentifiedObject.name"       → String
 *
 * ORGANISATION GATE:
 *   Currently CAISO only. Add to ENRICHMENT_ENABLED_ORGS to enable others.
 *
 * OOM FIX (2026-11-06):
 *   findDocs() and fetchPage() now accept field projections.
 *   Each call site projects ONLY the fields it needs — eliminates loading
 *   full documents (attributes + references + resolvedRefIds) into heap
 *   for 2M+ object jobs. Projections reduce per-document heap usage by ~90%.
 */
@Service
public class CimEnrichmentService {

    private static final Logger log = LoggerFactory.getLogger(CimEnrichmentService.class);

    private static final Set<String> ENRICHMENT_ENABLED_ORGS =
            new HashSet<>(Arrays.asList(
                "CAISO"
                // "ERCOT",
                // "SPC",
            ));

    // resolvedRefIds keys — MapKeySanitizer encodes dots as underscores (REPL="_")
    // ALL ref keys must use underscore to match what's stored in MongoDB after import.
    // REF_SM_GU was already underscore (confirmed from MongoDB before the REPL change).
    private static final String REF_CE_BV      = "ConductingEquipment_BaseVoltage";
    private static final String REF_T_EQUIP    = "Terminal_ConductingEquipment";
    private static final String REF_T_CN       = "Terminal_ConnectivityNode";
    private static final String REF_CN_VL      = "ConnectivityNode_ConnectivityNodeContainer";
    private static final String REF_VL_SUB     = "VoltageLevel_Substation";
    private static final String REF_VL_BV      = "VoltageLevel_BaseVoltage";
    private static final String REF_SUB_REGION = "Substation_Region";
    private static final String REF_SGR_REGION = "SubGeographicalRegion_Region";
    private static final String REF_SGR_ALT    = "Substation_Region";
    private static final String REF_SM_GU      = "SynchronousMachine_GeneratingUnit";
    // Name object refs — Name links a display name to any IdentifiedObject
    private static final String REF_NAME_IO   = "Name_IdentifiedObject";   // Name.IdentifiedObject
    private static final String ATTR_NAME_NAME = "Name_name";              // Name.name text value

    // attributes keys — MapKeySanitizer encodes dots as underscores (REPL="_")
    private static final String ATTR_VOLTAGE = "BaseVoltage_nominalVoltage";
    private static final String ATTR_NAME    = "IdentifiedObject_name";

    private static final int PAGE = 2000; // increased from 500 — fewer round-trips

    private final MongoTemplate mongo;

    public CimEnrichmentService(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    // ── Entry point ───────────────────────────────────────────────────────

    /**
     * Enriches CIM objects for the given job.
     *
     * @return Number of CIM objects that received at least one enriched attribute
     *         (nominalVoltage, substationMrid, friendlyName, etc.).
     *         Returns {@code -1} when enrichment is disabled for this org.
     */
    public long enrich(String jobId, String orgId) {
        if (!isEnabled(orgId)) {
            log.info("CimEnrichment skipped: org={} not in ENRICHMENT_ENABLED_ORGS", orgId);
            return -1L;
        }
        long start = System.currentTimeMillis();
        log.info("CimEnrichment starting: job={} org={}", jobId, orgId);

        Map<String, String>       bvMap     = buildBvMap(jobId);
        log.info("HEAP after bvMap: {}MB", Runtime.getRuntime().totalMemory()/1024/1024);
        // equipToCn replaces tByEquip + cnByT — single Terminal pass, equip → list of CN rdfIds
        Map<String, List<String>> equipToCn = buildEquipToCn(jobId);
        log.info("HEAP after equipToCn: {}MB", Runtime.getRuntime().totalMemory()/1024/1024);
        Map<String, String>       vlByCn    = buildVlByCn(jobId);
        log.info("HEAP after vlByCn: {}MB", Runtime.getRuntime().totalMemory()/1024/1024);
        Map<String, SubInfo>      subByVl   = buildSubByVl(jobId);
        log.info("HEAP after subByVl: {}MB", Runtime.getRuntime().totalMemory()/1024/1024);
        enrichWithRegions(jobId, subByVl);
        // GeneratingUnit → SynchronousMachine rdfId
        // (GeneratingUnit has no direct Terminal — must go via SynchronousMachine)
        Map<String, String>       smByGu    = buildSmByGu(jobId);
        log.info("HEAP after smByGu: {}MB", Runtime.getRuntime().totalMemory()/1024/1024);
        Map<String, String>       vlBvMap   = buildVlBvMap(jobId);
        log.info("HEAP after vlBvMap: {}MB", Runtime.getRuntime().totalMemory()/1024/1024);
        // friendlyNameMap skipped — too many Name objects for 256MB heap
        Map<String, String>       friendlyNameMap = new HashMap<>();

        log.info("Maps: bv={} equipToCn={} vlByCn={} subByVl={}",
                bvMap.size(), equipToCn.size(), vlByCn.size(), subByVl.size());

        long voltageCount = 0, substationCount = 0;
        long enrichedObjectCount = 0;   // unique objects that received ≥1 enriched attribute
        String lastId = null;

        while (true) {
            List<Document> page = fetchPage(jobId, lastId);
            if (page.isEmpty()) break;
            lastId = page.get(page.size() - 1).getObjectId("_id").toHexString();

            List<Map<String, Object>> updates = new ArrayList<>(page.size());
            for (Document doc : page) {
                Map<String, String> enriched = new LinkedHashMap<>();
                computeVoltage(doc, bvMap, smByGu, equipToCn, vlByCn, vlBvMap, enriched);
                computeSubstations(doc, equipToCn, vlByCn, subByVl, smByGu, friendlyNameMap, enriched);
                computeFriendlyName(doc, friendlyNameMap, enriched);
                if (enriched.isEmpty()) continue;
                if (enriched.containsKey("nominalVoltage")) voltageCount++;
                if (enriched.containsKey("substationMrid"))  substationCount++;
                enrichedObjectCount++;   // this object got at least one enriched field
                Map<String, Object> upd = new LinkedHashMap<>(2);
                upd.put("id",      doc.getObjectId("_id").toHexString()); // fixed: no duplicate put
                upd.put("enriched", enriched);
                updates.add(upd);
            }
            if (!updates.isEmpty()) bulkWrite(updates);
            if (voltageCount % 50000 == 0 && voltageCount > 0)
                log.info("CimEnrichment progress: nominalVoltage={} substation={} elapsed={}ms",
                        voltageCount, substationCount,
                        System.currentTimeMillis() - start);
        }

        log.info("CimEnrichment done: job={} org={} elapsed={} enrichedObjects={} nominalVoltage={} substation={}",
                jobId, orgId,
                com.cim.model.mongo.StageTimings.format(System.currentTimeMillis() - start),
                enrichedObjectCount, voltageCount, substationCount);
        return enrichedObjectCount;
    }

    private boolean isEnabled(String orgId) {
        if (orgId == null) return false;
        return ENRICHMENT_ENABLED_ORGS.stream()
                .anyMatch(o -> o.equalsIgnoreCase(orgId.trim()));
    }

    // ── Lookup maps — all use raw Document ───────────────────────────────

    /** BaseVoltage.rdfId → nominalVoltage string (handles numeric BSON value). */
    private Map<String, String> buildBvMap(String jobId) {
        Map<String, String> map = new HashMap<>();
        String lastId = null;
        while (true) {
            // Project only rdfId, mrid, and the one attribute we need
            List<Document> batch = findDocs(jobId, "BaseVoltage", lastId,
                    "_id", "rdfId", "mrid", "attributes." + ATTR_VOLTAGE);
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();
            for (Document doc : batch) {
                Document attrs = doc.get("attributes", Document.class);
                if (attrs == null) continue;
                String val = safeString(attrs.get(ATTR_VOLTAGE));
                if (val == null) continue;
                putBoth(map, doc.getString("rdfId"), doc.getString("mrid"), val);
            }
        }
        log.info("buildBvMap: {} entries", map.size());
        return map;
    }

    /**
     * Single Terminal pass: equipmentRdfId → list of ConnectivityNode rdfIds.
     * Replaces the two-map approach (tByEquip + cnByT) — eliminates the
     * intermediate Terminal rdfId strings from heap entirely.
     * Memory saving: ~100MB for 450K+ Terminal objects in CAISO.
     */
    private Map<String, List<String>> buildEquipToCn(String jobId) {
        Map<String, List<String>> map = new HashMap<>();
        String lastId = null;
        while (true) {
            // Project both ref keys in one pass — no need to store Terminal rdfId
            List<Document> batch = findDocs(jobId, "Terminal", lastId,
                    "_id", "resolvedRefIds." + REF_T_EQUIP, "resolvedRefIds." + REF_T_CN);
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();
            for (Document doc : batch) {
                Document refs = doc.get("resolvedRefIds", Document.class);
                if (refs == null) continue;
                String equipId = (String) refs.get(REF_T_EQUIP);
                String cnId    = (String) refs.get(REF_T_CN);
                if (equipId != null && !equipId.isBlank()
                        && cnId != null && !cnId.isBlank())
                    map.computeIfAbsent(equipId, k -> new ArrayList<>()).add(cnId);
            }
        }
        log.info("buildEquipToCn: {} entries", map.size());
        return map;
    }

    /** ConnectivityNode.rdfId → VoltageLevel.rdfId. */
    private Map<String, String> buildVlByCn(String jobId) {
        Map<String, String> map = new HashMap<>();
        String lastId = null;
        while (true) {
            // Only need rdfId + the one ref key pointing to VL
            List<Document> batch = findDocs(jobId, "ConnectivityNode", lastId,
                    "_id", "rdfId", "resolvedRefIds." + REF_CN_VL);
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();
            for (Document doc : batch) {
                String rdfId = doc.getString("rdfId");
                Document refs = doc.get("resolvedRefIds", Document.class);
                if (rdfId == null || refs == null) continue;
                String vlId = (String) refs.get(REF_CN_VL);
                if (vlId != null && !vlId.isBlank()) map.put(rdfId, vlId);
            }
        }
        log.info("buildVlByCn: {} entries", map.size());
        return map;
    }

    /** VoltageLevel.rdfId → SubInfo (rdfId, mrid, name). */
    private Map<String, SubInfo> buildSubByVl(String jobId) {
        // Step A: VL → substation rdfId
        Map<String, String> subRdfByVl = new HashMap<>();
        String lastId = null;
        while (true) {
            // Only need rdfId + the one ref key pointing to Substation
            List<Document> batch = findDocs(jobId, "VoltageLevel", lastId,
                    "_id", "rdfId", "resolvedRefIds." + REF_VL_SUB);
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();
            for (Document doc : batch) {
                String rdfId = doc.getString("rdfId");
                Document refs = doc.get("resolvedRefIds", Document.class);
                if (rdfId == null || refs == null) continue;
                String subId = (String) refs.get(REF_VL_SUB);
                if (subId != null && !subId.isBlank()) subRdfByVl.put(rdfId, subId);
            }
        }
        if (subRdfByVl.isEmpty()) return new HashMap<>();

        // Step B: fetch Substation mrid + name
        // BATCHED — query in chunks of 500 to avoid massive $in arrays
        // that cause MongoDB cursor timeouts (MongoInternalException on getMore).
        // Field projection limits the data transferred over the wire.
        Map<String, SubInfo> subByRdfId = new HashMap<>();
        List<String> subRdfIdList = new ArrayList<>(new HashSet<>(subRdfByVl.values()));
        int batchSize = 500;
        boolean logged = false;
        for (int i = 0; i < subRdfIdList.size(); i += batchSize) {
            List<String> chunk = subRdfIdList.subList(i,
                    Math.min(i + batchSize, subRdfIdList.size()));
            // VoltageLevel.Substation ref may point to Substation rdfId or mrid
            Query q = new Query(Criteria.where("jobId").is(jobId)
                    .and("cimType").is("Substation")
                    .orOperator(
                        Criteria.where("rdfId").in(chunk),
                        Criteria.where("mrid").in(chunk)
                    ));
            // Project only the three fields we need — reduces data over the wire
            q.fields()
                    .include("rdfId")
                    .include("mrid")
                    .include("attributes." + ATTR_NAME);
            for (Document doc : mongo.find(q, Document.class, "cim_objects")) {
                String rdfId = doc.getString("rdfId");
                if (rdfId == null) continue;
                Document attrs = doc.get("attributes", Document.class);
                String name = attrs != null ? safeString(attrs.get(ATTR_NAME)) : null;
                if (!logged) {
                    log.info("Substation sample: rdfId={} name={}", rdfId, name);
                    logged = true;
                }
                String mrid = doc.getString("mrid");
                // Use mrid as primary key for SubInfo so sgrRdfBySub lookup works
                // (Substation.Region ref stores the mrid value, not the rdfId)
                SubInfo si = new SubInfo(mrid != null ? mrid : rdfId, mrid, name);
                putBothSubInfo(subByRdfId, rdfId, mrid, si);
            }
        }

        // Step C: vlRdfId → SubInfo
        Map<String, SubInfo> result = new HashMap<>();
        for (Map.Entry<String, String> e : subRdfByVl.entrySet()) {
            SubInfo si = subByRdfId.get(e.getValue());
            if (si != null) result.put(e.getKey(), si);
        }
        log.info("buildSubByVl: {} entries", result.size());
        return result;
    }

    /** Enriches each SubInfo with SGR mrid/name and GR mrid/name. */
    private void enrichWithRegions(String jobId, Map<String, SubInfo> subByVl) {
        if (subByVl.isEmpty()) return;

        // Collect unique Substation identifiers (si.rdfId = mrid after buildSubByVl fix)
        Set<String> subMrids = subByVl.values().stream()
                .filter(s -> s.rdfId != null).map(s -> s.rdfId).collect(Collectors.toSet());

        // Substation → sgrRef via "Substation.Region"
        // Key sgrRdfBySub by mrid so the SubInfo lookup matches
        Map<String, String> sgrRdfBySub = new HashMap<>();
        // Batched — avoid large $in arrays causing cursor timeouts
        List<String> subMridList = new ArrayList<>(subMrids);
        for (int i = 0; i < subMridList.size(); i += 500) {
            List<String> chunk = subMridList.subList(i, Math.min(i + 500, subMridList.size()));
            Query subQ = new Query(Criteria.where("jobId").is(jobId)
                    .and("cimType").is("Substation")
                    .orOperator(
                        Criteria.where("rdfId").in(chunk),
                        Criteria.where("mrid").in(chunk)
                    ));
            subQ.fields().include("rdfId").include("mrid").include("resolvedRefIds." + REF_SUB_REGION);
            for (Document doc : mongo.find(subQ, Document.class, "cim_objects")) {
                Document refs = doc.get("resolvedRefIds", Document.class);
                if (refs == null) continue;
                String sgrId = (String) refs.get(REF_SUB_REGION);
                if (sgrId == null || sgrId.isBlank()) continue;
                String rdfId = doc.getString("rdfId");
                String mrid  = doc.getString("mrid");
                if (rdfId != null) sgrRdfBySub.put(rdfId, sgrId);
                if (mrid  != null && !mrid.equals(rdfId)) sgrRdfBySub.put(mrid, sgrId);
            }
        }

        Set<String> sgrRdfIds = new HashSet<>(sgrRdfBySub.values());
        if (sgrRdfIds.isEmpty()) return;

        // SGR → mrid, name, grRdfId
        // NOTE: Substation.Region ref value = SGR's MRID (not rdfId).
        // SGR documents store the string identifier in "mrid" field.
        // Query by mrid, not rdfId.
        Map<String, String[]> sgrInfo = new HashMap<>();
        // Batched SGR query
        boolean sgrLogged = false;
        List<String> sgrIdList = new ArrayList<>(sgrRdfIds);
        for (int i = 0; i < sgrIdList.size(); i += 500) {
            List<String> chunk = sgrIdList.subList(i, Math.min(i + 500, sgrIdList.size()));
            Query sgrQ = new Query(Criteria.where("jobId").is(jobId)
                    .and("cimType").is("SubGeographicalRegion").and("mrid").in(chunk));
            sgrQ.fields().include("rdfId").include("mrid")
                    .include("attributes." + ATTR_NAME)
                    .include("resolvedRefIds." + REF_SGR_REGION);
            for (Document doc : mongo.find(sgrQ, Document.class, "cim_objects")) {
                String mrid = doc.getString("mrid");
                if (mrid == null) continue;
                Document attrs = doc.get("attributes", Document.class);
                Document refs  = doc.get("resolvedRefIds", Document.class);
                String name    = attrs != null ? safeString(attrs.get(ATTR_NAME)) : null;
                String grId    = refs  != null ? (String) refs.get(REF_SGR_REGION) : null;
                if (grId == null && refs != null) grId = (String) refs.get(REF_SGR_ALT);
                if (!sgrLogged) {
                    log.info("SGR sample: mrid={} name={} grRef={}", mrid, name, grId);
                    sgrLogged = true;
                }
                String[] entry = {mrid, name, grId};
                sgrInfo.put(mrid, entry);
                String rdfId = doc.getString("rdfId");
                if (rdfId != null && !rdfId.equals(mrid)) sgrInfo.put(rdfId, entry);
            }
        }

        // GR → mrid, name
        Set<String> grRdfIds = sgrInfo.values().stream()
                .filter(v -> v[2] != null && !v[2].isBlank())
                .map(v -> v[2]).collect(Collectors.toSet());
        Map<String, String[]> grInfo = new HashMap<>();
        if (!grRdfIds.isEmpty()) {
            // GR: SubGeographicalRegion.Region ref value may be GR's mrid
            // Query by mrid OR rdfId — use $or to handle both cases
            // Batched GR query
            boolean grLogged = false;
            List<String> grIdList = new ArrayList<>(grRdfIds);
            for (int i = 0; i < grIdList.size(); i += 500) {
                List<String> chunk = grIdList.subList(i, Math.min(i + 500, grIdList.size()));
                Query grQ = new Query(Criteria.where("jobId").is(jobId)
                        .and("cimType").is("GeographicalRegion")
                        .orOperator(
                            Criteria.where("rdfId").in(chunk),
                            Criteria.where("mrid").in(chunk)
                        ));
                grQ.fields().include("rdfId").include("mrid").include("attributes." + ATTR_NAME);
                for (Document doc : mongo.find(grQ, Document.class, "cim_objects")) {
                    Document attrs = doc.get("attributes", Document.class);
                    String name  = attrs != null ? safeString(attrs.get(ATTR_NAME)) : null;
                    String rdfId = doc.getString("rdfId");
                    String mrid  = doc.getString("mrid");
                    if (!grLogged) {
                        log.info("GR sample: rdfId={} mrid={} name={}", rdfId, mrid, name);
                        grLogged = true;
                    }
                    String[] entry = {mrid, name};
                    if (rdfId != null) grInfo.put(rdfId, entry);
                    if (mrid  != null && !mrid.equals(rdfId)) grInfo.put(mrid, entry);
                }
            }
        }

        // Attach SGR + GR to each SubInfo
        long withSgr = 0, withGr = 0;
        for (SubInfo si : subByVl.values()) {
            // Try si.rdfId first (which is set to mrid in buildSubByVl), then si.mrid
            String sgrId = si.rdfId != null ? sgrRdfBySub.get(si.rdfId) : null;
            if (sgrId == null && si.mrid != null) sgrId = sgrRdfBySub.get(si.mrid);
            if (sgrId == null) continue;
            String[] sgr = sgrInfo.get(sgrId);
            if (sgr == null) continue;
            si.sgrMrid = sgr[0]; si.sgrName = sgr[1];
            withSgr++;
            if (sgr[2] != null) {
                String[] gr = grInfo.get(sgr[2]);
                if (gr != null) { si.grMrid = gr[0]; si.grName = gr[1]; withGr++; }
            }
        }
        log.info("enrichWithRegions: {}/{} have SGR, {}/{} have GR",
                withSgr, subByVl.size(), withGr, subByVl.size());
    }

    /**
     * Builds a map of: IdentifiedObject.rdfId → friendly display name
     */
    private Map<String, String> buildFriendlyNameMap(String jobId) {
        Map<String, String> map = new HashMap<>();
        String lastId = null;
        int pageSize = PAGE;
        while (true) {
            Query q = new Query(Criteria.where("jobId").is(jobId)
                    .and("cimType").is("Name"));
            if (lastId != null)
                q.addCriteria(Criteria.where("_id").gt(
                        new org.bson.types.ObjectId(lastId)));
            q.limit(pageSize)
             .with(org.springframework.data.domain.Sort.by("_id"));
            // Only fetch the two fields we need
            q.fields()
             .include("_id")
             .include("attributes." + ATTR_NAME_NAME)
             .include("resolvedRefIds." + REF_NAME_IO)
             .include("references." + REF_NAME_IO);  // fallback raw references

            List<Document> batch = mongo.find(q, Document.class, "cim_objects");
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();

            for (Document doc : batch) {
                Document attrs = doc.get("attributes", Document.class);
                if (attrs == null) continue;

                String nameText = safeString(attrs.get(ATTR_NAME_NAME));
                if (nameText == null || nameText.isBlank()) continue;

                // Try resolvedRefIds first (post-Phase 2)
                // Fall back to raw references (pre-Phase 2 or if Name not resolved)
                String targetRef = null;
                Document resolved = doc.get("resolvedRefIds", Document.class);
                if (resolved != null) targetRef = (String) resolved.get(REF_NAME_IO);

                // Fallback: read from raw references map
                if (targetRef == null || targetRef.isBlank()) {
                    Document rawRefs = doc.get("references", Document.class);
                    if (rawRefs != null) {
                        // references stores encoded key — try both dot and underscore
                        targetRef = (String) rawRefs.get(REF_NAME_IO);
                        if (targetRef == null)
                            targetRef = (String) rawRefs.get("Name.IdentifiedObject");
                    }
                }

                if (targetRef == null || targetRef.isBlank()) continue;

                // Clean ref — remove leading # if present
                if (targetRef.startsWith("#"))
                    targetRef = targetRef.substring(1);

                // Store by target rdfId — first name wins if multiple exist
                map.putIfAbsent(targetRef, nameText);
            }
        }
        log.info("buildFriendlyNameMap: {} Name→IdentifiedObject entries", map.size());
        return map;
    }

    /**
     * VoltageLevel.rdfId → BaseVoltage.rdfId
     * Used for GeneratingUnit voltage lookup via VL chain.
     */
    private Map<String, String> buildVlBvMap(String jobId) {
        Map<String, String> map = new HashMap<>();
        String lastId = null;
        while (true) {
            // Only need rdfId + the one ref key pointing to BaseVoltage
            List<Document> batch = findDocs(jobId, "VoltageLevel", lastId,
                    "_id", "rdfId", "resolvedRefIds." + REF_VL_BV);
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();
            for (Document doc : batch) {
                String rdfId = doc.getString("rdfId");
                Document refs = doc.get("resolvedRefIds", Document.class);
                if (rdfId == null || refs == null) continue;
                String bvId = (String) refs.get(REF_VL_BV);
                if (bvId != null && !bvId.isBlank()) map.put(rdfId, bvId);
            }
        }
        log.info("buildVlBvMap: {} VoltageLevel→BaseVoltage entries", map.size());
        return map;
    }

    /**
     * GeneratingUnit.rdfId → SynchronousMachine.rdfId
     */
    private Map<String, String> buildSmByGu(String jobId) {
        Map<String, String> map = new HashMap<>();
        String lastId = null;
        while (true) {
            // Only need rdfId + the one ref key pointing to GeneratingUnit
            List<Document> batch = findDocs(jobId, "SynchronousMachine", lastId,
                    "_id", "rdfId", "resolvedRefIds." + REF_SM_GU);
            if (batch.isEmpty()) break;
            lastId = batch.get(batch.size()-1).getObjectId("_id").toHexString();
            for (Document doc : batch) {
                String smRdfId = doc.getString("rdfId");
                Document refs  = doc.get("resolvedRefIds", Document.class);
                if (smRdfId == null || refs == null) continue;
                String guRdfId = (String) refs.get(REF_SM_GU);
                if (guRdfId != null && !guRdfId.isBlank())
                    map.put(guRdfId, smRdfId); // GU rdfId → SM rdfId
            }
        }
        log.info("buildSmByGu: {} GeneratingUnit→SynchronousMachine entries", map.size());
        return map;
    }

    /**
     * Writes <CimType>FriendlyName to the enriched map if a Name entry
     * exists for this object.
     */
    private void computeFriendlyName(Document doc,
                                      Map<String, String> friendlyNameMap,
                                      Map<String, String> enriched) {
        if (friendlyNameMap.isEmpty()) return;

        String rdfId   = doc.getString("rdfId");
        String mrid    = doc.getString("mrid");
        String cimType = doc.getString("cimType");
        if (cimType == null) return;

        // Look up by rdfId first, then mrid as fallback
        String friendlyName = null;
        if (rdfId != null) friendlyName = friendlyNameMap.get(rdfId);
        if (friendlyName == null && mrid != null && !mrid.equals(rdfId))
            friendlyName = friendlyNameMap.get(mrid);

        if (friendlyName == null || friendlyName.isBlank()) return;

        // Key: <CimType>FriendlyName  e.g. "SubstationFriendlyName"
        String key = cimType + "FriendlyName";
        enriched.put(key, friendlyName);
    }

    // ── Per-object computation ─────────────────────────────────────────────

    private void computeVoltage(Document doc,
                                 Map<String, String> bvMap,
                                 Map<String, String> smByGu,
                                 Map<String, List<String>> equipToCn,
                                 Map<String, String> vlByCn,
                                 Map<String, String> vlBvMap,
                                 Map<String, String> enriched) {
        Document refs = doc.get("resolvedRefIds", Document.class);

        // Path 1: direct ConductingEquipment.BaseVoltage ref (most equipment)
        String bvId = refs != null ? (String) refs.get(REF_CE_BV) : null;
        if (bvId != null && !bvId.isBlank()) {
            String voltage = bvMap.get(bvId);
            if (voltage != null && !voltage.isBlank()) {
                enriched.put("nominalVoltage", voltage);
                return;
            }
        }

        // Path 2: GeneratingUnit has no direct BaseVoltage ref.
        // Chain: GU → SM (via smByGu) → CN (via equipToCn) → VL → BaseVoltage
        String rdfId = doc.getString("rdfId");
        if (rdfId == null) return;
        String smRdfId = smByGu.get(rdfId);
        if (smRdfId == null) return;

        List<String> cnIds = equipToCn.get(smRdfId);
        if (cnIds == null || cnIds.isEmpty()) return;

        for (String cnId : cnIds) {
            String vlId = vlByCn.get(cnId); if (vlId == null) continue;
            String bvRdfId = vlBvMap.get(vlId);
            if (bvRdfId == null) continue;
            String voltage = bvMap.get(bvRdfId);
            if (voltage != null && !voltage.isBlank()) {
                enriched.put("nominalVoltage", voltage);
                return;
            }
        }
    }

    // CIM types that span multiple substations — skip substation/SGR/GR for these
    private static final Set<String> SKIP_SUBSTATION_TYPES =
            new HashSet<>(Arrays.asList("ACLineSegment", "PowerTransformer"));

    private void computeSubstations(Document doc,
                                     Map<String, List<String>> equipToCn,
                                     Map<String, String> vlByCn,
                                     Map<String, SubInfo> subByVl,
                                     Map<String, String> smByGu,
                                     Map<String, String> friendlyNameMap,
                                     Map<String, String> enriched) {
        // Skip ACLineSegment and PowerTransformer — they span multiple substations
        String cimType = doc.getString("cimType");
        if (cimType != null && SKIP_SUBSTATION_TYPES.contains(cimType)) return;

        String rdfId = doc.getString("rdfId");
        if (rdfId == null) return;

        // Try direct equip→CN lookup first
        List<String> cnIds = equipToCn.get(rdfId);

        // GeneratingUnit has no direct Terminal — look up via SynchronousMachine
        if ((cnIds == null || cnIds.isEmpty()) && smByGu.containsKey(rdfId)) {
            String smRdfId = smByGu.get(rdfId);
            cnIds = equipToCn.get(smRdfId);
        }

        if (cnIds == null || cnIds.isEmpty()) return;

        LinkedHashMap<String, SubInfo> found = new LinkedHashMap<>();
        for (String cnId : cnIds) {
            String vlId = vlByCn.get(cnId);  if (vlId == null) continue;
            SubInfo sub = subByVl.get(vlId); if (sub == null) continue;
            String key = sub.mrid != null ? sub.mrid : sub.rdfId;
            if (key != null) found.put(key, sub);
        }
        if (found.isEmpty()) return;

        // Naming convention (manager requirement):
        //   First instance  : no suffix  → substationMrid, substationName
        //   Second instance : suffix "B" → substationMridB, substationNameB
        //   Third instance  : suffix "C" → substationMridC, substationNameC
        //   ... and so on through the alphabet
        //
        // Same pattern for subGeographicalRegion and geographicalRegion.
        // GeographicalRegion is deduplicated — written once even if multiple
        // substations share the same GR.

        // Build deduplicated SGR and GR maps preserving insertion order
        java.util.LinkedHashMap<String, String> sgrNameByMrid = new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String, String> grNameByMrid  = new java.util.LinkedHashMap<>();

        int idx = 0;
        for (SubInfo sub : found.values()) {
            // suffix: idx=0 → "", idx=1 → "B", idx=2 → "C", ...
            String suffix = idx == 0 ? "" : String.valueOf((char) ('B' + idx - 1));

            if (sub.mrid != null) enriched.put("substationMrid" + suffix, sub.mrid);
            if (sub.name != null) enriched.put("substationName" + suffix, sub.name);
            // FriendlyName from Name object — keyed with same suffix for multi-substation
            String subFriendly = friendlyNameMap.get(sub.rdfId);
            if (subFriendly == null && sub.mrid != null)
                subFriendly = friendlyNameMap.get(sub.mrid);
            if (subFriendly != null && !subFriendly.isBlank())
                enriched.put("substationFriendlyName" + suffix, subFriendly);

            // Collect SGR/GR for deduplication
            if (sub.sgrMrid != null && !sub.sgrMrid.isBlank())
                sgrNameByMrid.merge(sub.sgrMrid,
                        sub.sgrName != null ? sub.sgrName : "",
                        (a, b) -> a.isEmpty() ? b : a);
            if (sub.grMrid != null && !sub.grMrid.isBlank())
                grNameByMrid.merge(sub.grMrid,
                        sub.grName != null ? sub.grName : "",
                        (a, b) -> a.isEmpty() ? b : a);
            idx++;
        }

        // Write deduplicated SGR with same A/B/C suffix pattern
        int sgrIdx = 0;
        for (java.util.Map.Entry<String, String> e : sgrNameByMrid.entrySet()) {
            if (e.getKey().isBlank()) { sgrIdx++; continue; }
            String suffix = sgrIdx == 0 ? "" : String.valueOf((char) ('B' + sgrIdx - 1));
            enriched.put("subGeographicalRegionMrid" + suffix, e.getKey());
            if (!e.getValue().isBlank())
                enriched.put("subGeographicalRegionName" + suffix, e.getValue());
            sgrIdx++;
        }

        // Write deduplicated GR with same A/B/C suffix pattern
        int grIdx = 0;
        for (java.util.Map.Entry<String, String> e : grNameByMrid.entrySet()) {
            if (e.getKey().isBlank()) { grIdx++; continue; }
            String suffix = grIdx == 0 ? "" : String.valueOf((char) ('B' + grIdx - 1));
            enriched.put("geographicalRegionMrid" + suffix, e.getKey());
            if (!e.getValue().isBlank())
                enriched.put("geographicalRegionName" + suffix, e.getValue());
            grIdx++;
        }
    }

    // ── Write ──────────────────────────────────────────────────────────────

    private void bulkWrite(List<Map<String, Object>> updates) {
        if (updates == null || updates.isEmpty()) return;
        try {
            org.springframework.data.mongodb.core.BulkOperations bulkOps =
                    mongo.bulkOps(
                            org.springframework.data.mongodb.core.BulkOperations.BulkMode.UNORDERED,
                            "cim_objects");
            int added = 0;
            for (Map<String, Object> upd : updates) {
                String id = (String) upd.get("id");
                if (id == null) continue;
                @SuppressWarnings("unchecked")
                Map<String, String> enriched = (Map<String, String>) upd.get("enriched");
                if (enriched == null || enriched.isEmpty()) continue;
                Update update = new Update();
                for (Map.Entry<String, String> e : enriched.entrySet())
                    update.set("attributes." + e.getKey(), e.getValue());
                bulkOps.updateOne(
                    new Query(Criteria.where("_id").is(new org.bson.types.ObjectId(id))),
                    update);
                added++;
            }
            if (added > 0) {
                org.springframework.data.mongodb.core.BulkWriteResult result =
                        bulkOps.execute();
                log.info("bulkWrite: {} updates sent, {} modified",
                        added, result.getModifiedCount());
            }
        } catch (Exception ex) {
            log.warn("bulkWrite batch error: {} — falling back to individual updates",
                    ex.getMessage());
            for (Map<String, Object> upd : updates) {
                String id = (String) upd.get("id");
                if (id == null) continue;
                @SuppressWarnings("unchecked")
                Map<String, String> enriched = (Map<String, String>) upd.get("enriched");
                if (enriched == null || enriched.isEmpty()) continue;
                try {
                    Update update = new Update();
                    for (Map.Entry<String, String> e : enriched.entrySet())
                        update.set("attributes." + e.getKey(), e.getValue());
                    mongo.updateFirst(
                        new Query(Criteria.where("_id")
                                .is(new org.bson.types.ObjectId(id))),
                        update, "cim_objects");
                } catch (Exception e2) {
                    log.warn("bulkWrite fallback error id={}: {}", id, e2.getMessage());
                }
            }
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /**
     * Fetches a page of cim_objects for the given jobId.
     * Projects only the fields needed by compute* methods — avoids loading
     * full documents (attributes, raw references) into heap for 2M+ objects.
     *
     * Fields included:
     *   _id, rdfId, mrid, cimType  — identity fields used by all compute* methods
     *   resolvedRefIds              — ref lookups (ConductingEquipment.BaseVoltage etc.)
     * NOTE: full "attributes" is NOT projected here — enrichment reads voltage/name
     *       from the pre-built in-memory maps, not from each document's attributes.
     */
    private List<Document> fetchPage(String jobId, String lastId) {
        Criteria c = Criteria.where("jobId").is(jobId);
        if (lastId != null)
            c = new Criteria().andOperator(c,
                    Criteria.where("_id").gt(new org.bson.types.ObjectId(lastId)));
        Query q = new Query(c).limit(PAGE).with(Sort.by(Sort.Direction.ASC, "_id"));
        q.fields()
            .include("_id")
            .include("rdfId")
            .include("mrid")
            .include("cimType")
            .include("resolvedRefIds");
        return mongo.find(q, Document.class, "cim_objects");
    }

    /**
     * Fetches documents of a given cimType for map-building, projecting only
     * the specified fields. Caller specifies exactly what it needs — avoids
     * loading full documents into heap.
     *
     * Always includes "_id" for cursor pagination regardless of fields passed.
     */
    private List<Document> findDocs(String jobId, String cimType,
                                     String lastId, String... fields) {
        Criteria c = Criteria.where("jobId").is(jobId).and("cimType").is(cimType);
        if (lastId != null)
            c = new Criteria().andOperator(c,
                    Criteria.where("_id").gt(new org.bson.types.ObjectId(lastId)));
        Query q = new Query(c).limit(PAGE).with(Sort.by(Sort.Direction.ASC, "_id"));
        if (fields != null && fields.length > 0) {
            for (String f : fields) q.fields().include(f);
        }
        return mongo.find(q, Document.class, "cim_objects");
    }

    private void putBoth(Map<String, String> map, String rdfId, String mrid, String val) {
        if (rdfId != null && !rdfId.isBlank()) map.put(rdfId, val);
        if (mrid != null && !mrid.isBlank() && !mrid.equals(rdfId)) map.put(mrid, val);
    }

    private void putBothSubInfo(Map<String, SubInfo> map, String rdfId, String mrid, SubInfo si) {
        if (rdfId != null && !rdfId.isBlank()) map.put(rdfId, si);
        if (mrid != null && !mrid.isBlank() && !mrid.equals(rdfId)) map.put(mrid, si);
    }

    /** Converts any BSON value (String, Double, Integer, Boolean) to String. */
    private String safeString(Object val) {
        if (val == null) return null;
        String s = val.toString().trim();
        return s.isBlank() ? null : s;
    }

    private static class SubInfo {
        final String rdfId, mrid, name;
        String sgrMrid, sgrName, grMrid, grName;
        SubInfo(String rdfId, String mrid, String name) {
            this.rdfId = rdfId; this.mrid = mrid; this.name = name;
        }
    }
}
