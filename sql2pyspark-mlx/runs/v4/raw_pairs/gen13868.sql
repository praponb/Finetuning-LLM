WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS discount_pct_share,
        NTILE(5) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM promotions
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    discount_pct_bucket
FROM metrics
