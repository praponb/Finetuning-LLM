WITH with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        NTILE(5) OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket
    FROM promotions
    WHERE promo_code IN ('BUNDLE', 'SPRING10')
)
SELECT
    CASE WHEN discount_pct_cume_dist >= 0.75 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_cume_dist_tier,
    AVG(discount_pct_bucket) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_avg_discount_pct_bucket,
    MAX(discount_pct_cume_dist) AS max_discount_pct_cume_dist,
    COUNT(CASE WHEN promo_code = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT_IF(channel = 'web') AS web_events
FROM with_windows
GROUP BY discount_pct_cume_dist_tier
HAVING max_discount_pct_cume_dist >= 0
ORDER BY bundle_avg_discount_pct_bucket;
