package com.cim.service;

import com.cim.model.mongo.CimObjectRecord;
import com.cim.repository.CimObjectRecordRepository;
import com.cim.repository.ValidationJobRepository;
import com.cim.util.MapKeySanitizer;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates a GeoJSON FeatureCollection from a job's {@code cim_objects},
 * one Point Feature per record.  Coordinates are converted from North
 * Carolina State Plane (NAD83, US Survey Feet — EPSG:2264) to WGS84
 * longitude/latitude using a hardcoded Lambert Conformal Conic inverse.
 *
 * <h2>Why this exists</h2>
 * Downstream visualisation tools (web maps, GIS, the legacy topology
 * processor shown in the reference screenshots) consume the network as a
 * GeoJSON {@code FeatureCollection} of named, typed Points with
 * parent-pointer topology.  This service produces that file directly from
 * the parsed CIM data so the visualisation layer doesn't need its own
 * importer.
 *
 * <h2>Output shape</h2>
 * <pre>
 * {
 *   "type": "FeatureCollection",
 *   "features": [
 *     {
 *       "type": "Feature",
 *       "geometry": { "type": "Point", "coordinates": [-78.756, 36.093, 0] },
 *       "properties": {
 *         "Name": "BU",
 *         "Type": "Busbar",
 *         "Phase": 7,
 *         "ParentName": "ROOT"
 *       }
 *     },
 *     ...
 *   ]
 * }
 * </pre>
 *
 * <h2>cimType → Type label mapping</h2>
 * The {@code Type} property uses the legacy/utility labels rather than the
 * raw CIM class name — matching the existing GeoJSON consumers.  See
 * {@link #LABEL_FOR_CIM_TYPE}.  Anything outside that table is passed
 * through verbatim so unmapped data is visible rather than silently lost.
 *
 * <h2>Coordinate system caveat</h2>
 * The Lambert Conformal Conic constants are hardcoded for NC State Plane
 * (NAD83).  Files imported from other states will produce <em>wrong</em>
 * lat/long output — the X/Y numbers come through the inverse projection
 * with the wrong assumptions and land far from the source location.  A
 * startup sanity check ({@link #verifyProjection()}) converts a known
 * Wake-County point and logs a warning if the result is off — but it
 * doesn't fail the service.
 *
 * <h2>Streaming output</h2>
 * Features are written using a Jackson streaming generator so a 116k-row
 * job (Wake) doesn't materialise the whole FeatureCollection in memory.
 * MongoDB is paged at {@link #BATCH} rows per fetch.
 */
@Service
public class GeoJsonExportService {

    private static final Logger log =
            LoggerFactory.getLogger(GeoJsonExportService.class);

    private static final int BATCH = 500;

    /** Where files land.  Subdir per jobId so re-runs don't collide. */
    private static final String OUTPUT_ROOT = "/tmp/cim-exports";
    private static final String OUTPUT_FILENAME = "wake-topology.geojson";

    /**
     * cimType → GeoJSON {@code "Type"} label.  Order chosen to match the
     * legacy GeoJSON output examples; values are passed through verbatim
     * for any cimType not on this list.
     */
    private static final Map<String, String> LABEL_FOR_CIM_TYPE = new HashMap<>();
    static {
        LABEL_FOR_CIM_TYPE.put("EnergySource",           "Busbar");
        LABEL_FOR_CIM_TYPE.put("BusbarSection",          "Busbar");
        LABEL_FOR_CIM_TYPE.put("RatioTapChanger",        "Regulator");
        LABEL_FOR_CIM_TYPE.put("ProtectedSwitch",        "Feeder");
        LABEL_FOR_CIM_TYPE.put("Breaker",                "Feeder");
        LABEL_FOR_CIM_TYPE.put("Recloser",               "Feeder");
        LABEL_FOR_CIM_TYPE.put("Sectionaliser",          "Feeder");
        LABEL_FOR_CIM_TYPE.put("Fuse",                   "Feeder");
        LABEL_FOR_CIM_TYPE.put("Disconnector",           "Feeder");
        LABEL_FOR_CIM_TYPE.put("LoadBreakSwitch",        "Switch");
        LABEL_FOR_CIM_TYPE.put("Switch",                 "Switch");
        LABEL_FOR_CIM_TYPE.put("UsagePoint",             "Meter");
        LABEL_FOR_CIM_TYPE.put("ACLineSegment",          "Line");
        LABEL_FOR_CIM_TYPE.put("PowerTransformer",       "Transformer");
        LABEL_FOR_CIM_TYPE.put("LinearShuntCompensator", "Capacitor");
        LABEL_FOR_CIM_TYPE.put("ConnectivityNode",       "Node");
        LABEL_FOR_CIM_TYPE.put("EnergyConsumer",         "Load");
        LABEL_FOR_CIM_TYPE.put("SynchronousMachine",     "Generator");
        LABEL_FOR_CIM_TYPE.put("AsynchronousMachine",    "Motor");
    }

    /** Attribute keys we read from cim_objects (dot-encoded as stored). */
    private static final String KEY_NAME    = MapKeySanitizer.encode("IdentifiedObject.name");
    private static final String KEY_X       = MapKeySanitizer.encode("PositionPoint.xPosition");
    private static final String KEY_Y       = MapKeySanitizer.encode("PositionPoint.yPosition");
    private static final String KEY_PHASE   = MapKeySanitizer.encode("Milsoft.phaseCode");
    private static final String KEY_PARENT  = MapKeySanitizer.encode("Milsoft.parent");
    private static final String KEY_PARENT2 = MapKeySanitizer.encode("Milsoft.parentId");
    // Used to refine ACLineSegment → OverheadLine / UndergroundLine in the
    // GeoJSON output.  Milsoft.lineType is set explicitly by the .STD parser
    // ("overhead" / "underground"); Milsoft.sectionType holds the raw type
    // code as a string ("1" for OH, "3" for UG).  We prefer the explicit
    // tag and fall back to the code when only that is present.
    private static final String KEY_LINE_TYPE    = MapKeySanitizer.encode("Milsoft.lineType");
    private static final String KEY_SECTION_TYPE = MapKeySanitizer.encode("Milsoft.sectionType");

    private final CimObjectRecordRepository repo;
    private final ValidationJobRepository    jobRepo;
    private final NcStatePlaneProjector      projector;

    public GeoJsonExportService(CimObjectRecordRepository repo,
                                 ValidationJobRepository jobRepo) {
        this.repo      = repo;
        this.jobRepo   = jobRepo;
        this.projector = new NcStatePlaneProjector();
        verifyProjection();   // logs a warning if the math is off
    }

    /**
     * Generate GeoJSON for one job.  Returns a result holder with the
     * output path and counts.  Idempotent — overwrites the previous file
     * for the same jobId.  Does not throw on per-row errors; rows that
     * can't be processed are counted as skipped.
     */
    public GeoJsonResult generate(String jobId) {
        long t0 = System.currentTimeMillis();
        var job = jobRepo.findByJobId(jobId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Job not found: " + jobId));

        Path outDir  = Paths.get(OUTPUT_ROOT, jobId);
        Path outFile = outDir.resolve(OUTPUT_FILENAME);

        long featuresWritten = 0;
        long skippedNoCoords = 0;
        long skippedBadCoords = 0;
        long totalExamined = 0;

        try {
            Files.createDirectories(outDir);
        } catch (IOException e) {
            throw new RuntimeException("Cannot create output dir " + outDir, e);
        }

        // Streaming Jackson writer — never materialises the full collection.
        JsonFactory factory = new JsonFactory();
        try (OutputStream os = Files.newOutputStream(outFile);
             JsonGenerator g = factory.createGenerator(os, JsonEncoding.UTF8)) {

            g.useDefaultPrettyPrinter();

            g.writeStartObject();
            g.writeStringField("type", "FeatureCollection");
            g.writeFieldName("features");
            g.writeStartArray();

            // Page through cim_objects for this job.
            int page = 0;
            while (true) {
                var slice = repo.findByJobId(jobId, PageRequest.of(page, BATCH));
                if (slice.isEmpty()) break;
                for (CimObjectRecord rec : slice.getContent()) {
                    totalExamined++;
                    Map<String,String> attrs = rec.getAttributes();
                    if (attrs == null) attrs = Collections.emptyMap();

                    String xRaw = attrs.get(KEY_X);
                    String yRaw = attrs.get(KEY_Y);
                    if (isBlank(xRaw) || isBlank(yRaw)) {
                        skippedNoCoords++;
                        continue;
                    }

                    double xFeet, yFeet;
                    try {
                        xFeet = Double.parseDouble(xRaw.trim());
                        yFeet = Double.parseDouble(yRaw.trim());
                    } catch (NumberFormatException nfe) {
                        skippedBadCoords++;
                        continue;
                    }

                    // Project to WGS84.  Inverse can fail near the standard
                    // parallels mathematically (rare) or for nonsense input.
                    double[] lonLat;
                    try {
                        lonLat = projector.toWgs84(xFeet, yFeet);
                    } catch (Exception ex) {
                        skippedBadCoords++;
                        continue;
                    }

                    writeFeature(g, rec, attrs, lonLat[0], lonLat[1]);
                    featuresWritten++;
                }
                if (!slice.hasNext()) break;
                page++;
            }

            g.writeEndArray();   // features
            g.writeEndObject();  // root
            g.flush();
        } catch (IOException e) {
            throw new RuntimeException("GeoJSON write failed for job " + jobId, e);
        }

        long elapsed = System.currentTimeMillis() - t0;
        log.info("GeoJSON written job={} file={} features={} skippedNoCoords={} "
                + "skippedBadCoords={} examined={} elapsedMs={}",
                jobId, outFile, featuresWritten, skippedNoCoords,
                skippedBadCoords, totalExamined, elapsed);

        return new GeoJsonResult(jobId, outFile.toString(), featuresWritten,
                skippedNoCoords, skippedBadCoords, totalExamined, elapsed);
    }

    private void writeFeature(JsonGenerator g, CimObjectRecord rec,
                               Map<String,String> attrs,
                               double lon, double lat) throws IOException {
        g.writeStartObject();
        g.writeStringField("type", "Feature");

        // ── geometry ──────────────────────────────────────────────────
        g.writeFieldName("geometry");
        g.writeStartObject();
        g.writeStringField("type", "Point");
        g.writeFieldName("coordinates");
        g.writeStartArray();
        g.writeNumber(lon);
        g.writeNumber(lat);
        g.writeNumber(0);    // legacy GeoJSON examples include altitude=0
        g.writeEndArray();
        g.writeEndObject();

        // ── properties ────────────────────────────────────────────────
        g.writeFieldName("properties");
        g.writeStartObject();

        // Name — prefer the record's name, fall back to rdfId.
        String name = rec.getName();
        if (isBlank(name)) name = attrs.get(KEY_NAME);
        if (isBlank(name)) name = rec.getRdfId();
        g.writeStringField("Name", name != null ? name : "");

        // Type — use the legacy label for this cimType, with one refinement:
        // ACLineSegment is split into OverheadLine vs UndergroundLine using
        // Milsoft.lineType (preferred) or Milsoft.sectionType code (1=OH, 3=UG)
        // as a fallback.  Other cimTypes fall back to a verbatim passthrough.
        String label = resolveTypeLabel(rec.getCimType(), attrs);
        g.writeStringField("Type", label != null ? label : "");

        // Phase — numeric if parseable, else string.  Milsoft.phaseCode is
        // a string in the source file ("1"/"7"/etc.) but the legacy GeoJSON
        // writes it as a JSON number.
        String phaseStr = attrs.get(KEY_PHASE);
        if (!isBlank(phaseStr)) {
            try {
                g.writeNumberField("Phase", Integer.parseInt(phaseStr.trim()));
            } catch (NumberFormatException nfe) {
                g.writeStringField("Phase", phaseStr);
            }
        }

        // ParentName — comes from references.  Format depends on whether
        // the multi-valued refactor has been integrated:
        //   • Map<String, String>       → string value
        //   • Map<String, List<String>> → list value, take first
        // We handle both defensively so this service compiles either way.
        String parent = extractParent(rec);
        g.writeStringField("ParentName", parent != null ? parent : "ROOT");

        g.writeEndObject();   // properties
        g.writeEndObject();   // feature
    }

    /**
     * Resolve the GeoJSON "Type" label for a record.
     *
     * <p>Most CIM types map one-to-one via {@link #LABEL_FOR_CIM_TYPE}, but
     * {@code ACLineSegment} is special: in CIM, both overhead and underground
     * lines share the same class, distinguished only by an auxiliary
     * attribute.  The legacy GeoJSON consumers (and Wake's reference file)
     * expect them split into {@code "OverheadLine"} vs {@code "UndergroundLine"}.
     *
     * <p>Resolution order for ACLineSegment:
     * <ol>
     *   <li>{@code Milsoft.lineType} attribute — "overhead" → OverheadLine,
     *       "underground" → UndergroundLine (the .STD parser sets this).</li>
     *   <li>{@code Milsoft.sectionType} attribute — raw section code; "1" →
     *       OverheadLine, "3" → UndergroundLine (per WindMil v5 spec).</li>
     *   <li>If neither attribute is present, default to {@code "Line"} (the
     *       previous behaviour).</li>
     * </ol>
     *
     * <p>For non-ACLineSegment cimTypes, this is a wrapped lookup of
     * {@link #LABEL_FOR_CIM_TYPE} with a verbatim passthrough.
     */
    private String resolveTypeLabel(String cimType, Map<String,String> attrs) {
        if ("ACLineSegment".equals(cimType)) {
            String lineType = attrs.get(KEY_LINE_TYPE);
            if (lineType != null) {
                String lt = lineType.trim().toLowerCase();
                if (lt.startsWith("over") || "oh".equals(lt))  return "OverheadLine";
                if (lt.startsWith("under") || "ug".equals(lt)) return "UndergroundLine";
            }
            String code = attrs.get(KEY_SECTION_TYPE);
            if (code != null) {
                String c = code.trim();
                if ("1".equals(c)) return "OverheadLine";
                if ("3".equals(c)) return "UndergroundLine";
            }
            return "Line";
        }
        return LABEL_FOR_CIM_TYPE.getOrDefault(cimType, cimType);
    }

    /**
     * Read the Milsoft.parent reference value, defensively handling both
     * the old shape (Map&lt;String,String&gt;) and the new multi-valued
     * shape (Map&lt;String,List&lt;String&gt;&gt;).  Returns null if no
     * parent reference exists.
     *
     * <p><b>Integration note:</b> if the multi-valued refactor IS
     * integrated, the {@code getReferences()} return type changes and the
     * cast below will compile but at runtime see a {@code List} value
     * where it used to see a {@code String}.  The instanceof branches
     * cover both cases without a code change.
     */
    private String extractParent(CimObjectRecord rec) {
        @SuppressWarnings("rawtypes")
        Map refs = rec.getReferences();
        if (refs == null || refs.isEmpty()) return null;
        Object v = refs.get(KEY_PARENT);
        if (v == null) v = refs.get(KEY_PARENT2);
        if (v == null) return null;
        if (v instanceof String) {
            String s = ((String) v).trim();
            return s.isEmpty() ? null : s;
        }
        if (v instanceof List) {
            List<?> lst = (List<?>) v;
            if (lst.isEmpty()) return null;
            Object first = lst.get(0);
            return first == null ? null : first.toString();
        }
        // Unknown shape — log once and move on.
        return v.toString();
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    /**
     * Sanity-check the hardcoded NC State Plane constants against a known
     * Wake-County point.  The {@code BU} EnergySource at state-plane
     * {@code (2071886.23919, 853078.1418)} should land near WGS84
     * {@code (-78.7566937829, 36.0936509423)} — the value visible in the
     * legacy GeoJSON sample.
     *
     * <p>If the converted coordinates are more than ~0.001° (≈ 100 m) off,
     * the math is wrong (likely a constant transcription error or a
     * misunderstanding of the foot type).  We log a WARN rather than
     * throw, because the service is still useful for non-Wake data even
     * if the verification fails — but operators should investigate.
     */
    private void verifyProjection() {
        try {
            double[] r = projector.toWgs84(2071886.23919, 853078.1418);
            double dLon = Math.abs(r[0] - (-78.7566937829));
            double dLat = Math.abs(r[1] -   36.0936509423);
            if (dLon > 0.001 || dLat > 0.001) {
                log.warn("NC State Plane projection sanity check FAILED: "
                        + "expected ~(-78.7567, 36.0937), got ({}, {}). "
                        + "GeoJSON coordinates will be wrong.  Review the "
                        + "constants in NcStatePlaneProjector.",
                        r[0], r[1]);
            } else {
                log.info("NC State Plane projection sanity check OK "
                       + "(BU node converts to ({}, {}))", r[0], r[1]);
            }
        } catch (Exception e) {
            log.warn("NC State Plane projection sanity check threw: {}",
                    e.getMessage());
        }
    }

    /**
     * Lambert Conformal Conic (LCC) inverse — North Carolina State Plane,
     * NAD83, US Survey Feet (EPSG:2264).
     *
     * <p>Constants per NCDOT / EPSG:
     * <ul>
     *   <li>Standard parallels: φ1 = 34°20′ N, φ2 = 36°10′ N</li>
     *   <li>Latitude of origin: φ0 = 33°45′ N</li>
     *   <li>Longitude of origin: λ0 = 79°00′ W</li>
     *   <li>False easting:  609 601.22 m (= 2 000 000 US survey feet)</li>
     *   <li>False northing: 0 m</li>
     *   <li>Ellipsoid: GRS80 (NAD83) — a = 6 378 137 m, e² = 0.00669438002290</li>
     * </ul>
     *
     * <p>Math reference: Snyder, "Map Projections — A Working Manual"
     * (USGS Professional Paper 1395), §15 "Lambert Conformal Conic
     * Projection".  Inverse formulas eqs. (15-4) through (15-11).
     *
     * <p>NAD83 → WGS84 datum shift is ignored (~1 m in this region — fine
     * for utility-scale visualisation, not for sub-metre GPS work).
     */
    static final class NcStatePlaneProjector {

        // GRS80 / NAD83 ellipsoid.
        private static final double A   = 6378137.0;                  // semi-major (m)
        private static final double E2  = 0.00669438002290;           // eccentricity squared
        private static final double E   = Math.sqrt(E2);

        // NC State Plane standard parallels and origin.
        private static final double PHI_1 = Math.toRadians(34 + 20.0/60.0); // 34°20′
        private static final double PHI_2 = Math.toRadians(36 + 10.0/60.0); // 36°10′
        private static final double PHI_0 = Math.toRadians(33 + 45.0/60.0); // 33°45′
        private static final double LAM_0 = Math.toRadians(-79.0);          // 79°W

        // False origin (metres).
        private static final double FALSE_E_M = 609601.22;
        private static final double FALSE_N_M = 0.0;

        // US Survey Foot → metre.  Note: NOT the international foot 0.3048;
        // NC State Plane is defined in US Survey Feet (1 ft = 1200/3937 m).
        private static final double SURVEY_FT_TO_M = 1200.0 / 3937.0;

        // Pre-computed projection constants n, F, ρ0.  Snyder eq. (15-1),
        // (15-2), (15-1a) using the t function (15-9) and m function (14-15).
        private static final double N;
        private static final double F;
        private static final double RHO_0;

        static {
            double m1 = m(PHI_1);
            double m2 = m(PHI_2);
            double t1 = t(PHI_1);
            double t2 = t(PHI_2);
            double t0 = t(PHI_0);
            N = (Math.log(m1) - Math.log(m2)) / (Math.log(t1) - Math.log(t2));
            F = m1 / (N * Math.pow(t1, N));
            RHO_0 = A * F * Math.pow(t0, N);
        }

        /** Snyder eq. (14-15): m = cos φ / sqrt(1 − e² sin² φ). */
        private static double m(double phi) {
            double s = Math.sin(phi);
            return Math.cos(phi) / Math.sqrt(1.0 - E2 * s * s);
        }

        /** Snyder eq. (15-9): t = tan(π/4 − φ/2) / [(1 − e sin φ)/(1 + e sin φ)]^(e/2). */
        private static double t(double phi) {
            double s = Math.sin(phi);
            double a = Math.tan(Math.PI/4.0 - phi/2.0);
            double b = (1.0 - E*s) / (1.0 + E*s);
            return a / Math.pow(b, E/2.0);
        }

        /**
         * Inverse projection: (easting, northing) in US Survey Feet →
         * (longitude, latitude) in WGS84 decimal degrees.  Returns
         * {@code double[]{lon, lat}}.
         *
         * @throws ArithmeticException for inputs that fail to converge
         *         (very rare — happens only for points hundreds of km
         *         outside the projection's reasonable area of use).
         */
        public double[] toWgs84(double xFeet, double yFeet) {
            // Survey feet → metres, then subtract false origin.
            double x = xFeet * SURVEY_FT_TO_M - FALSE_E_M;
            double y = yFeet * SURVEY_FT_TO_M - FALSE_N_M;

            // Snyder eq. (15-10): ρ = sign(n) × √(x² + (ρ0 − y)²)
            double dy = RHO_0 - y;
            double rho = Math.copySign(Math.sqrt(x*x + dy*dy), N);

            // Snyder eq. (15-11): θ = atan2(x, ρ0 − y).
            double theta = Math.atan2(x, dy);

            // Snyder eq. (15-8): t = (ρ / (a × F))^(1/n).
            double tVal = Math.pow(rho / (A * F), 1.0 / N);

            // Solve for φ iteratively from t (Snyder eq. 7-9 / 15-4).
            // Initial estimate: φ = π/2 − 2 arctan t.  Then refine.
            double phi = Math.PI/2.0 - 2.0 * Math.atan(tVal);
            for (int i = 0; i < 25; i++) {
                double s = Math.sin(phi);
                double next = Math.PI/2.0 - 2.0 * Math.atan(
                        tVal * Math.pow((1.0 - E*s) / (1.0 + E*s), E/2.0));
                if (Math.abs(next - phi) < 1.0e-12) {
                    phi = next; break;
                }
                phi = next;
            }

            // λ = θ/n + λ0.
            double lam = theta / N + LAM_0;

            return new double[]{ Math.toDegrees(lam), Math.toDegrees(phi) };
        }
    }

    /** Public result holder for endpoints and callers. */
    public static final class GeoJsonResult {
        public final String jobId;
        public final String filePath;
        public final long   featuresWritten;
        public final long   skippedNoCoords;
        public final long   skippedBadCoords;
        public final long   examined;
        public final long   elapsedMs;

        public GeoJsonResult(String jobId, String filePath,
                              long featuresWritten, long skippedNoCoords,
                              long skippedBadCoords, long examined,
                              long elapsedMs) {
            this.jobId            = jobId;
            this.filePath         = filePath;
            this.featuresWritten  = featuresWritten;
            this.skippedNoCoords  = skippedNoCoords;
            this.skippedBadCoords = skippedBadCoords;
            this.examined         = examined;
            this.elapsedMs        = elapsedMs;
        }
    }
}
