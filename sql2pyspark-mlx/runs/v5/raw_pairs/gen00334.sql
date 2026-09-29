WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id ASC) AS latest_rank
    FROM promotions
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    promo_code,
    DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
    CASE WHEN discount_pct >= 0.26 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS discount_pct_per_web,
    COUNT(*) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_rows,
    SIZE(COLLECT_SET(channel)) AS num_channels
FROM ranked
GROUP BY ROLLUP (promo_code, DATE_TRUNC('QUARTER', ends_on), CASE WHEN discount_pct >= 0.26 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY quarter_start DESC;
