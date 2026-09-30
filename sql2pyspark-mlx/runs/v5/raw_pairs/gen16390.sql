WITH valid AS (
    SELECT
        *,
        DATE_SUB(ends_on, 88) AS ends_on_minus_88d,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE discount_pct BETWEEN 0.19 AND 0.32
)
SELECT
    DATE_TRUNC('YEAR', ends_on_minus_88d) AS year_start,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    MAX(discount_pct) FILTER (WHERE promo_code = 'SPRING10') AS spring10_max_discount_pct
FROM valid
GROUP BY DATE_TRUNC('YEAR', ends_on_minus_88d)
HAVING num_promo_codes > 1
ORDER BY year_start DESC NULLS FIRST;
