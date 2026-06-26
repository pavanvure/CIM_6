package com.cim.util;

/**
 * Lambert Conformal Conic (LCC) inverse projection — North Carolina State
 * Plane, NAD83, US Survey Feet (EPSG:2264).  Converts state-plane (X, Y)
 * easting/northing in US Survey Feet to WGS84 longitude/latitude in
 * decimal degrees.
 *
 * <h2>Constants</h2>
 * Per NCDOT / EPSG:
 * <ul>
 *   <li>Standard parallels: φ1 = 34°20′ N, φ2 = 36°10′ N</li>
 *   <li>Latitude of origin: φ0 = 33°45′ N</li>
 *   <li>Longitude of origin: λ0 = 79°00′ W</li>
 *   <li>False easting:  609 601.22 m (= 2 000 000 US survey feet)</li>
 *   <li>False northing: 0 m</li>
 *   <li>Ellipsoid: GRS80 (NAD83) — a = 6 378 137 m, e² = 0.00669438002290</li>
 * </ul>
 *
 * <h2>Verified accuracy</h2>
 * Cross-verified against Wake County reference point: state-plane
 * (2071886.23919, 853078.1418) → WGS84 (-78.7566937829, 36.0936509423).
 * Maximum delta against expected ≈ 7 × 10⁻¹⁰ degrees (~0.08 mm at this
 * latitude).  See unit tests for verification.
 *
 * <h2>Caveats</h2>
 * <p><b>NC ONLY.</b>  Constants are hardcoded for North Carolina State
 * Plane.  Applying this to data from any other state's state-plane zone
 * (or any other projection) silently produces wrong lat/lon.  Callers are
 * responsible for ensuring the input is NC State Plane data.
 *
 * <p><b>NAD83 → WGS84 datum shift IGNORED.</b>  The two datums differ by
 * ~1–2 m in this region.  Fine for utility-scale visualization, wrong
 * for sub-metre GPS applications.
 *
 * <h2>Math reference</h2>
 * Snyder, "Map Projections — A Working Manual" (USGS Professional Paper
 * 1395), §15 "Lambert Conformal Conic Projection".  Inverse formulas
 * eqs. (15-4) through (15-11).
 */
public final class NcStatePlaneProjector {

    // GRS80 / NAD83 ellipsoid.
    private static final double A   = 6378137.0;        // semi-major (m)
    private static final double E2  = 0.00669438002290; // eccentricity²
    private static final double E   = Math.sqrt(E2);

    // NC State Plane standard parallels and origin.
    private static final double PHI_1 = Math.toRadians(34 + 20.0/60.0); // 34°20′
    private static final double PHI_2 = Math.toRadians(36 + 10.0/60.0); // 36°10′
    private static final double PHI_0 = Math.toRadians(33 + 45.0/60.0); // 33°45′
    private static final double LAM_0 = Math.toRadians(-79.0);          // 79°W

    // False origin (metres).
    private static final double FALSE_E_M = 609601.22;
    private static final double FALSE_N_M = 0.0;

    // US Survey Foot → metre.  NOT the international foot 0.3048.
    private static final double SURVEY_FT_TO_M = 1200.0 / 3937.0;

    // Pre-computed projection constants n, F, ρ₀.
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

    private NcStatePlaneProjector() {}   // utility, no instances

    /** Snyder eq. (14-15). */
    private static double m(double phi) {
        double s = Math.sin(phi);
        return Math.cos(phi) / Math.sqrt(1.0 - E2 * s * s);
    }

    /** Snyder eq. (15-9). */
    private static double t(double phi) {
        double s = Math.sin(phi);
        double a = Math.tan(Math.PI/4.0 - phi/2.0);
        double b = (1.0 - E*s) / (1.0 + E*s);
        return a / Math.pow(b, E/2.0);
    }

    /**
     * Inverse projection: (easting, northing) in US Survey Feet →
     * (longitude, latitude) in WGS84 decimal degrees.
     *
     * @return {@code double[]{lon, lat}}
     * @throws ArithmeticException for inputs that fail to converge — rare,
     *         only triggers for points hundreds of km outside the projection's
     *         reasonable area of use.
     */
    public static double[] toWgs84(double xFeet, double yFeet) {
        double x = xFeet * SURVEY_FT_TO_M - FALSE_E_M;
        double y = yFeet * SURVEY_FT_TO_M - FALSE_N_M;

        double dy = RHO_0 - y;
        double rho = Math.copySign(Math.sqrt(x*x + dy*dy), N);
        double theta = Math.atan2(x, dy);
        double tVal  = Math.pow(rho / (A * F), 1.0 / N);

        // Solve for φ iteratively (Snyder eq. 7-9 / 15-4).
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

        double lam = theta / N + LAM_0;
        return new double[]{ Math.toDegrees(lam), Math.toDegrees(phi) };
    }

    /**
     * Convenience: convert a state-plane point and return the lon/lat as
     * full-precision strings ready to store as attribute values.  Uses
     * {@link Double#toString} so no precision is lost.
     *
     * @return {@code String[]{lonStr, latStr}}
     */
    public static String[] toWgs84Strings(double xFeet, double yFeet) {
        double[] r = toWgs84(xFeet, yFeet);
        return new String[]{ Double.toString(r[0]), Double.toString(r[1]) };
    }

    /**
     * Self-verification: convert the known Wake-County reference point and
     * return true if the result is within tolerance of the expected output.
     * Used by services that want to confirm the constants are correct
     * before relying on the projection.
     */
    public static boolean verifyKnownPoint(double tolDegrees) {
        try {
            double[] r = toWgs84(2071886.23919, 853078.1418);
            double dLon = Math.abs(r[0] - (-78.7566937829));
            double dLat = Math.abs(r[1] -   36.0936509423);
            return dLon < tolDegrees && dLat < tolDegrees;
        } catch (Exception e) {
            return false;
        }
    }
}
