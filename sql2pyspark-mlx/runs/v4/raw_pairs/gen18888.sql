WITH cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        TRIM(channel) AS clean_channel,
        ROUND(discount_pct, 1) AS discount_pct_rounded,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM promotions
    WHERE ends_on > DATE '2024-03-26'
)
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    promo_code,
    PERCENTILE_APPROX(discount_pct_rounded, 0.95) AS discount_pct_rounded_p95,
    COUNT(*) FILTER (WHERE channel = 'partner') AS partner_rows,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    GROUPING(DATE_TRUNC('MONTH', ends_on)) AS grouping_level
FROM cleaned
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', ends_on), promo_code), (DATE_TRUNC('MONTH', ends_on)), ())
HAVING discount_pct_stddev >= 0
ORDER BY discount_pct_stddev DESC NULLS LAST
