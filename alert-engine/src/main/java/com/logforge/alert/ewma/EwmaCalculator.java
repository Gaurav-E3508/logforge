package com.logforge.alert.ewma;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Exponentially Weighted Moving Average (EWMA) calculator.
 *
 * REAL LIFE ANALOGY:
 * Imagine you're tracking how hot your coffee usually is.
 * You don't want to overreact to one unusually hot cup.
 * EWMA says: "my estimate = 30% today's temperature + 70% my previous estimate"
 * Over many cups, the estimate smoothly tracks the true average.
 * One outlier barely moves the needle — a genuine trend does.
 *
 * FORMULA:
 *   EWMA(t) = α × value(t) + (1 - α) × EWMA(t-1)
 *
 *   α = smoothing factor (0.3 here):
 *   - High α (0.8): reacts fast to changes, more noisy
 *   - Low  α (0.1): slow to react, very smooth, ignores spikes
 *   - 0.3: balanced — catches real trends, ignores single blips
 *
 * ANOMALY DETECTION:
 * Track both EWMA (mean) and variance.
 * Anomaly = current value > mean + (sensitivity × stdDev)
 *
 * Example:
 *   Historical avg error rate: 5%  (EWMA mean)
 *   Std deviation:             2%
 *   Sensitivity multiplier:    3x
 *   Anomaly threshold:         5 + (3 × 2) = 11%
 *   Current value:             47% → ANOMALY!
 */
@Slf4j
@Getter
public class EwmaCalculator {

    private final double alpha;          // smoothing factor
    private final String metricName;

    private double ewmaMean     = -1;    // -1 = not initialized yet
    private double ewmaVariance =  0;
    private long   updateCount  =  0;

    // Warm-up period — don't alert until we have enough data points
    private static final int WARMUP_PERIODS = 10;

    public EwmaCalculator(String metricName, double alpha) {
        this.metricName = metricName;
        this.alpha      = alpha;
    }

    /**
     * Update EWMA with a new observed value.
     * Call this once per evaluation interval (e.g., every minute).
     *
     * @return the updated EWMA mean
     */
    public double update(double newValue) {
        if (ewmaMean < 0) {
            // First data point — initialize
            ewmaMean     = newValue;
            ewmaVariance = 0;
        } else {
            // EWMA mean update
            double delta     = newValue - ewmaMean;
            ewmaMean        += alpha * delta;

            // EWMA variance update (Welford's online algorithm adapted for EWMA)
            ewmaVariance = (1 - alpha) * (ewmaVariance + alpha * delta * delta);
        }

        updateCount++;

        log.debug("EWMA update [{}] — value={:.2f}, mean={:.2f}, stdDev={:.2f}",
                metricName, newValue, ewmaMean, getStdDev());

        return ewmaMean;
    }

    /**
     * Check if a value is anomalous given current EWMA statistics.
     *
     * @param value            current observed value
     * @param sensitivitySigma how many standard deviations = anomaly (default 3.0)
     * @return true if value is statistically anomalous
     */
    public boolean isAnomaly(double value, double sensitivitySigma) {
        if (!isWarmedUp()) return false; // not enough data yet

        double threshold = ewmaMean + (sensitivitySigma * getStdDev());

        // Also check lower bound for sudden drops (e.g. service went completely silent)
        double lowerThreshold = ewmaMean - (sensitivitySigma * getStdDev());

        boolean anomaly = value > threshold || value < lowerThreshold;

        if (anomaly) {
            log.debug("ANOMALY [{}] — value={:.2f}, mean={:.2f}, " +
                            "threshold=[{:.2f}, {:.2f}]",
                    metricName, value, ewmaMean, lowerThreshold, threshold);
        }

        return anomaly;
    }

    public double getStdDev()    { return Math.sqrt(ewmaVariance); }
    public boolean isWarmedUp()  { return updateCount >= WARMUP_PERIODS; }
    public double  getThreshold(double sigma) {
        return ewmaMean + (sigma * getStdDev());
    }
}