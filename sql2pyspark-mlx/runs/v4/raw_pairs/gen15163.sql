WITH remaining_keys AS (
    -- Set difference on keys
    SELECT channel, promo_code
    FROM promotions
    EXCEPT
    SELECT channel, promo_code
    FROM promotions
    WHERE MONTH(ends_on) IN (7, 9, 12)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN remaining_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct >= 0.27 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    channel,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    SUM(CASE WHEN promo_code = 'BUNDLE' THEN discount_pct ELSE 0 END) AS bundle_discount_pct,
    SUM(CASE WHEN promo_code = 'WELCOME5' THEN discount_pct ELSE 0 END) AS welcome5_discount_pct,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows
FROM verified
GROUP BY GROUPING SETS ((CASE WHEN discount_pct >= 0.27 THEN 'HIGH' ELSE 'LOW' END, channel), (CASE WHEN discount_pct >= 0.27 THEN 'HIGH' ELSE 'LOW' END))
HAVING num_promo_codes >= 1
ORDER BY welcome5_discount_pct DESC NULLS FIRST;
