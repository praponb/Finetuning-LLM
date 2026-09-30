SELECT
    DATE_TRUNC('WEEK', starts_on) AS week_start,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(DISTINCT channel) AS distinct_channels,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev_2,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
FROM promotions
GROUP BY CUBE (DATE_TRUNC('WEEK', starts_on))
ORDER BY num_promo_codes DESC NULLS FIRST
