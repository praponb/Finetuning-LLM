WITH remaining_keys AS (
    -- Set difference on keys
    SELECT channel
    FROM promotions
    EXCEPT ALL
    SELECT channel
    FROM promotions
    WHERE promo_code IN ('BUNDLE', 'VIP')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN remaining_keys k ON s.channel = k.channel
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', ends_on) AS week_start,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END) AS vip_count,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles
FROM reconciled
GROUP BY week_start;
