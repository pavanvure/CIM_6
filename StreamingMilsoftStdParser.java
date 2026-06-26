package com.cim.streaming;

import com.cim.model.cim.CIMObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Milsoft WindMil ".STD" (Standard ASCII) parser — streaming, O(1) memory.
 *
 * This is the REAL WindMil export format (distinct from the INI-style
 * sectioned CSV handled by {@link StreamingMilsoftCsvParser}).
 *
 * FILE SHAPE (see file_layouts.pdf, "Section File Layout (.STD)"):
 *
 *   Line 1 (optional banner):
 *     MILSOFT STD WM ASCII,001    :&lt;ModelName&gt; Exported by WindMil on ...
 *
 *   Each subsequent line = ONE section/device.  Comma-delimited, NOT fixed
 *   width.  Empty field = two consecutive commas (use default).  First four
 *   fields are mandatory on every row:
 *
 *     col 0  A-1  Section Identifier   (unique name, e.g. "span_10289")
 *     col 1  A-2  Section Type         (integer code, e.g. 1, 9, 10)
 *     col 2  A-3  Phase Configuration  (1..7 bitmask; 7 = ABC)
 *     col 3  A-4  Prior/Parent Id      ("ROOT" or "0" = no parent)
 *     col 4  A-5  Map Number
 *     col 5  A-6  X Coordinate
 *     col 6  A-7  Y Coordinate
 *     col 7  A-8  Preferred Section Name
 *     col 8+      type-dependent fields (interpreted per Section Type)
 *
 * SECTION TYPE → CIM class (grounded in file_layouts.pdf + PNNL-34946):
 *     1  Overhead Line      → ACLineSegment
 *     2  Capacitor          → LinearShuntCompensator
 *     3  Underground Line   → ACLineSegment
 *     4  Regulator          → RatioTapChanger
 *     5  Step Transformer   → PowerTransformer
 *     6  Electric Switch    → Switch
 *     8  Node/Fake          → ConnectivityNode
 *     9  Source             → EnergySource
 *     10 Overcurrent Device → ProtectedSwitch  (recloser/breaker/sectionaliser/fuse)
 *     11 Motor              → AsynchronousMachine
 *     12 Generator          → SynchronousMachine
 *     0  End of Section     → skipped (mapping-system marker, not a device)
 *     7  (undefined)        → skipped
 *
 * IDENTITY / TOPOLOGY:
 *   rdfId = col 0 (the raw Milsoft identifier, verbatim).  The parent link
 *   (col 3) is emitted as a reference whose value is the raw parent id, so
 *   StreamingReferenceResolver matches it against the parent row's rdfId
 *   (foreignField:'rdfId') within the same jobId.  "ROOT"/"0" parents emit
 *   no reference.
 *
 * ATTRIBUTE KEYS:
 *   Universal fields → CIM paths under the default (cim) namespace, e.g.
 *     IdentifiedObject.name, PositionPoint.xPosition, ACDCTerminal.phases.
 *   Everything raw/positional is also preserved as Milsoft.col_&lt;n&gt; so
 *   raw_objects keeps the full file losslessly.  Type-specific named fields
 *   are added where the spec makes them unambiguous (see decorate*()).
 *
 *   Note: vendor attributes use the "Milsoft." (capital M, dot) form so they
 *   share the default cim-namespace prefix extraction (no ':' → treated as
 *   cim).  If you want them under a dedicated "milsoft:" namespace, change
 *   PFX below to "milsoft:" and seed that namespace as enabled.
 */
@Service
public class StreamingMilsoftStdParser {

    private static final Logger log =
            LoggerFactory.getLogger(StreamingMilsoftStdParser.class);

    /** Vendor attribute prefix.  See class note. */
    private static final String PFX = "Milsoft.";

    private static final Map<Integer, String> TYPE_TO_CIM = new HashMap<>();
    static {
        TYPE_TO_CIM.put(1,  "ACLineSegment");
        TYPE_TO_CIM.put(2,  "LinearShuntCompensator");
        TYPE_TO_CIM.put(3,  "ACLineSegment");
        TYPE_TO_CIM.put(4,  "RatioTapChanger");
        TYPE_TO_CIM.put(5,  "PowerTransformer");
        TYPE_TO_CIM.put(6,  "Switch");
        TYPE_TO_CIM.put(8,  "ConnectivityNode");
        TYPE_TO_CIM.put(9,  "EnergySource");
        TYPE_TO_CIM.put(10, "ProtectedSwitch");
        TYPE_TO_CIM.put(11, "AsynchronousMachine");
        TYPE_TO_CIM.put(12, "SynchronousMachine");
        // Type 13: service point / customer connection — post-2001 spec
        // extension.  Verified empirically: rows have type=13, single phase,
        // a PowerTransformer parent, X/Y coordinates, and all-zero electrical
        // fields.  That fits CIM UsagePoint (the metering / customer-side
        // delivery point, distinct from EndDevice which would be the meter
        // hardware itself).  The Milsoft section ID doubles as the billing
        // reference linking back to the .ACC file.
        TYPE_TO_CIM.put(13, "UsagePoint");
        // 0 and 7 intentionally absent → rows skipped.
    }

    /** Phase config code (col 2) → CIM ABC phase string. */
    private static final Map<Integer, String> PHASE_MAP = new HashMap<>();
    static {
        PHASE_MAP.put(1, "A");
        PHASE_MAP.put(2, "B");
        PHASE_MAP.put(3, "C");
        PHASE_MAP.put(4, "AB");
        PHASE_MAP.put(5, "AC");
        PHASE_MAP.put(6, "BC");
        PHASE_MAP.put(7, "ABC");
    }

    public void stream(InputStream in, String fileName,
                       Consumer<ParsedRow> onObject) throws Exception {
        long total = 0, skipped = 0;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8), 1 << 16)) {

            String line;
            int lineNum = 0;
            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.isEmpty()) continue;

                // Skip the banner row (first data field is the literal
                // "MILSOFT STD WM ASCII" token).
                if (lineNum == 1 && line.toUpperCase().contains("MILSOFT STD WM ASCII")) {
                    continue;
                }

                try {
                    ParsedRow pr = parseRow(line, fileName, lineNum);
                    if (pr != null) {
                        onObject.accept(pr);
                        total++;
                    } else {
                        skipped++;
                    }
                } catch (Exception e) {
                    skipped++;
                    log.warn("STD parse error line {}: {}", lineNum, e.getMessage());
                }
            }
        }
        log.info("Milsoft .STD parse complete: {} objects ({} skipped) from {}",
                total, skipped, fileName);
    }

    /**
     * Parse one data row into a {@link ParsedRow} carrying two views:
     * <ul>
     *   <li>{@code raw} — Milsoft-shaped, destined for {@code raw_objects}.
     *       cimType = the section code as a string ("9", "4", "10", ...).
     *       Attributes: only {@code Milsoft.col_<n>} positional dumps,
     *       {@code Milsoft.sectionType}, {@code Milsoft.phaseCode},
     *       {@code Milsoft.parentId}.  No CIM-class attribute paths.</li>
     *   <li>{@code cim} — CIM-translated view, destined for {@code cim_objects}
     *       via namespace filtering.  cimType = real CIM class
     *       ({@code EnergySource}, {@code ACLineSegment}, etc.).  Attributes:
     *       CIM-named paths only ({@code IdentifiedObject.name},
     *       {@code ACDCTerminal.phases}, {@code PositionPoint.xPosition},
     *       {@code PositionPoint.lon}, etc.) plus refinement tags
     *       ({@code Milsoft.lineType}, {@code Milsoft.sectionType}) that
     *       downstream consumers like the GeoJSON service use to refine the
     *       label.</li>
     * </ul>
     * Both views share the same {@code rdfId} so they can be correlated.
     *
     * <p>The {@code Milsoft.parent} reference is kept on BOTH sides because:
     * <ul>
     *   <li>raw needs it for re-validation (revalidate rebuilds cim_objects
     *       from raw_objects, including topology resolution).</li>
     *   <li>cim needs it so the reference resolver and Neo4j export wire
     *       the topology graph correctly.</li>
     * </ul>
     */
    private ParsedRow parseRow(String line, String fileName, int lineNum) {
        String[] f = splitCsv(line);
        if (f.length < 4) return null;

        String id        = trim(f[0]);
        String typeRaw   = trim(f[1]);
        String phaseRaw  = trim(f[2]);
        String parentRaw = trim(f[3]);

        if (id.isEmpty()) return null;

        Integer typeCode = parseIntOrNull(typeRaw);
        if (typeCode == null) return null;
        if (typeCode == 0 || typeCode == 7) return null;

        boolean hasParent = !parentRaw.isEmpty()
                && !"ROOT".equalsIgnoreCase(parentRaw)
                && !"0".equals(parentRaw);

        // ── Build the RAW view ─────────────────────────────────────────────
        // No CIM class assignment, no CIM attribute paths.  cimType is the
        // section code as a string so nothing here pretends to be CIM.  This
        // is what goes into raw_objects — full audit fidelity to the file.
        CIMObject raw = new CIMObject(typeRaw, id);
        raw.setSourceFile(fileName);
        raw.setSourceFormat("MILSOFT_STD");

        if (hasParent) {
            raw.addReference(PFX + "parent", parentRaw);
            raw.setAttribute(PFX + "parentId", parentRaw);
        }
        raw.setAttribute(PFX + "sectionType", typeRaw);
        if (!phaseRaw.isEmpty()) raw.setAttribute(PFX + "phaseCode", phaseRaw);

        // Map number — vendor-internal grouping, stays on raw only.
        if (f.length > 4 && !trim(f[4]).isEmpty()) {
            raw.setAttribute(PFX + "mapNumber", trim(f[4]));
        }

        // Every column verbatim → Milsoft.col_<i>, raw side only.  Preserves
        // the original row layout for the audit collection.
        for (int i = 0; i < f.length; i++) {
            String v = trim(f[i]);
            if (!v.isEmpty()) raw.setAttribute(PFX + "col_" + i, v);
        }

        // ── Build the CIM view ─────────────────────────────────────────────
        // CIM class assigned from the type code; attribute names follow CIM
        // conventions only.  No Milsoft.col_<n> clutter — the CIM collection
        // reads as CIM data, not as a Milsoft dump.
        String cimType = TYPE_TO_CIM.get(typeCode);
        if (cimType == null) {
            // Unknown but numeric type — keep it under a vendor-prefixed
            // pseudo-class so the row still surfaces in cim_objects and
            // operators can grep for "MilsoftType<n>" to find rows needing
            // a mapping entry.
            cimType = "MilsoftType" + typeCode;
        }

        CIMObject cim = new CIMObject(cimType, id);
        cim.setSourceFile(fileName);
        cim.setSourceFormat("MILSOFT_STD");

        // Identity — name (col 7 Preferred Section Name, else identifier).
        String preferred = f.length > 7 ? trim(f[7]) : "";
        cim.setAttribute("IdentifiedObject.name",
                preferred.isEmpty() ? id : preferred);

        // Phases via CIM PhaseCode literals (ABC, A, BC, etc.).
        Integer phaseCode = parseIntOrNull(phaseRaw);
        if (phaseCode != null && PHASE_MAP.containsKey(phaseCode)) {
            cim.setAttribute("ACDCTerminal.phases", PHASE_MAP.get(phaseCode));
        }

        // Section type — kept on CIM too for refinement tooling.  The GeoJSON
        // service uses this (combined with Milsoft.lineType from decorateByType
        // for code 1/3) to split ACLineSegment into OverheadLine vs
        // UndergroundLine.  Treated as a CIM-side refinement annotation, not
        // mainline CIM data.
        cim.setAttribute(PFX + "sectionType", typeRaw);

        // Parent topology link — kept on CIM so the reference resolver and
        // topology synthesis pass can wire connectivity.
        if (hasParent) {
            cim.addReference(PFX + "parent", parentRaw);
        }

        // Coordinates — see comment block in old single-emission version.
        // The CIM view carries BOTH the raw state-plane and the converted
        // WGS84 lon/lat.  Raw state-plane stays on the CIM side because
        // PositionPoint.xPosition/yPosition are CIM property paths; only
        // the Milsoft.col_<n> fallback (raw-only) duplicates them positionally.
        // Conversion errors are flagged ON THE CIM SIDE via Milsoft.coordError
        // — that's where the GeoJSON service looks.
        boolean hasX = f.length > 5 && !trim(f[5]).isEmpty();
        boolean hasY = f.length > 6 && !trim(f[6]).isEmpty();
        if (hasX) cim.setAttribute("PositionPoint.xPosition", trim(f[5]));
        if (hasY) cim.setAttribute("PositionPoint.yPosition", trim(f[6]));
        if (hasX && hasY) {
            try {
                double xFt = Double.parseDouble(trim(f[5]));
                double yFt = Double.parseDouble(trim(f[6]));
                String[] lonLat =
                        com.cim.util.NcStatePlaneProjector.toWgs84Strings(xFt, yFt);
                cim.setAttribute("PositionPoint.lon", lonLat[0]);
                cim.setAttribute("PositionPoint.lat", lonLat[1]);
            } catch (Exception ex) {
                // Sentinel zeros + error flag.  GeoJSON service checks
                // Milsoft.coordError and skips so features don't land at (0,0).
                cim.setAttribute("PositionPoint.lon", "0");
                cim.setAttribute("PositionPoint.lat", "0");
                cim.setAttribute(PFX + "coordError", ex.getMessage() == null
                        ? "conversion-failed" : ex.getMessage());
            }
        }

        // Type-specific decoration — adds named CIM attributes for fields
        // whose CIM meaning is unambiguous.  Operates on the CIM side only;
        // raw has the data positionally via Milsoft.col_<n>.
        decorateByType(cim, typeCode, f);

        return new ParsedRow(raw, cim);
    }

    /**
     * Add a few well-understood named fields per Section Type.  Conservative:
     * only fields whose meaning is unambiguous from the spec are named; the
     * rest remain available as Milsoft.col_&lt;n&gt;.
     */
    private void decorateByType(CIMObject o, int type, String[] f) {
        switch (type) {
            case 1:  // Overhead Line
            case 3:  // Underground Line
                // col 8/9/10 = per-phase conductor descriptions (equipment-DB labels)
                putIf(o, f, 8,  PFX + "conductorA");
                putIf(o, f, 9,  PFX + "conductorB");
                putIf(o, f, 10, PFX + "conductorC");
                putIf(o, f, 11, PFX + "conductorNeutral");
                // col 12 = impedance length in FEET
                putIf(o, f, 12, "Conductor.length");
                putIf(o, f, 13, PFX + "constructionType");
                o.setAttribute(PFX + "lineType", type == 1 ? "overhead" : "underground");
                break;
            case 9:  // Source
                // S-12 bus voltage (pu), S-15 nominal voltage (kV)
                putIf(o, f, 11, PFX + "busVoltagePU");
                putIf(o, f, 14, "EnergySource.nominalVoltage");
                putIf(o, f, 16, PFX + "wyeDelta");
                putIf(o, f, 17, PFX + "regulationCode");
                break;
            case 10: // Overcurrent Device (recloser/breaker/sectionaliser/fuse)
                putIf(o, f, 8,  PFX + "deviceA");
                putIf(o, f, 9,  PFX + "deviceB");
                putIf(o, f, 10, PFX + "deviceC");
                // O-12/13/14 = is-closed per phase (1 = closed). normalOpen is
                // the inverse of "closed" for the first existing phase.
                String closedA = f.length > 11 ? trim(f[11]) : "";
                if (!closedA.isEmpty()) {
                    boolean closed = "1".equals(closedA) || "true".equalsIgnoreCase(closedA);
                    o.setAttribute("Switch.normalOpen", closed ? "false" : "true");
                    o.setAttribute(PFX + "isClosedA", closedA);
                }
                break;
            case 6:  // Electric Switch — E-9 status O/C/L
                String status = f.length > 8 ? trim(f[8]) : "";
                if (!status.isEmpty()) {
                    // Spec: O=Open, C=Closed, L=Looped. Map to normalOpen.
                    boolean open = status.equalsIgnoreCase("O");
                    o.setAttribute("Switch.normalOpen", open ? "true" : "false");
                    o.setAttribute(PFX + "switchStatus", status);
                }
                break;
            case 5:  // Step Transformer
                putIf(o, f, 10, PFX + "ratedInputVoltage");
                putIf(o, f, 13, PFX + "ratedOutputVoltage");
                break;
            case 4:  // Regulator
                putIf(o, f, 8,  PFX + "regulatorType");
                putIf(o, f, 9,  PFX + "controllingPhase");
                break;
            case 13: // Service point / customer connection (post-2001 extension)
                // No type-specific electrical fields — service points carry only
                // identity, parent (the service transformer), phase, and
                // coordinates, all of which the universal A-1..A-8 handler above
                // has already captured.  The Milsoft section ID doubles as the
                // billing reference linking back to the .ACC file; surface it
                // explicitly under aliasName so queries can join on it without
                // peeking at the dot-encoded col_0 attribute.
                if (f.length > 0) {
                    String billingRef = trim(f[0]);
                    if (!billingRef.isEmpty()) {
                        o.setAttribute("IdentifiedObject.aliasName", billingRef);
                    }
                }
                break;
            default:
                // No named decoration; col_<n> already captures everything.
                break;
        }
    }

    private void putIf(CIMObject o, String[] f, int idx, String key) {
        if (f.length > idx) {
            String v = trim(f[idx]);
            if (!v.isEmpty()) o.setAttribute(key, v);
        }
    }

    /** Returns true if this looks like a WindMil .STD file (banner present). */
    public static boolean looksLikeStd(String firstLine) {
        return firstLine != null
                && firstLine.toUpperCase().contains("MILSOFT STD WM ASCII");
    }

    // ── CSV splitter (handles basic double-quote quoting) ─────────────────
    private String[] splitCsv(String line) {
        java.util.List<String> out = new java.util.ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQ = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQ = !inQ;
            } else if (c == ',' && !inQ) {
                out.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        out.add(sb.toString());
        return out.toArray(new String[0]);
    }

    private static String trim(String s) { return s == null ? "" : s.trim(); }

    private static Integer parseIntOrNull(String s) {
        if (s == null || s.isEmpty()) return null;
        try { return Integer.valueOf(s.trim()); }
        catch (NumberFormatException e) { return null; }
    }
}
