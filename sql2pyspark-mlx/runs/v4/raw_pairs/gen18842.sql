WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY starts_on DESC, id ASC) AS rn
        FROM promotions
    ) ranked
    WHERE rn = 1
)
-- Multi-level aggregation
SELECT
    promo_code,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    PERCENTILE_APPROX(discount_pct, 0.95) AS discount_pct_p95,
    COUNT_IF(promo_code = 'VIP') AS vip_events
FROM ranked
GROUP BY promo_code
