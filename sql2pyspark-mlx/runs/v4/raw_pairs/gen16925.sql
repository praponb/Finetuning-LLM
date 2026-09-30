WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
    FROM promotions
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct_bucket
FROM metrics
ORDER BY id DESC NULLS FIRST
