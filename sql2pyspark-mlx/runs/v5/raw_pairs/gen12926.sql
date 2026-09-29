WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, promo_code
    FROM promotions
    EXCEPT
    SELECT id, promo_code
    FROM promotions
    WHERE YEAR(starts_on) = 2023
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN matched_keys k ON s.id = k.id AND s.promo_code = k.promo_code
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    channel,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    AVG(discount_pct) FILTER (WHERE channel = 'store') AS store_avg_discount_pct,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN discount_pct >= 0.36 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY AVG(discount_pct) FILTER (WHERE channel = 'store') DESC) AS store_avg_discount_pct_dense_rank
FROM kept_rows
GROUP BY 1, 2
HAVING AVG(discount_pct) FILTER (WHERE channel = 'store') > 0
ORDER BY channel DESC
