WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS dedup_rank
    FROM promotions
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    channel,
    promo_code,
    discount_pct,
    dedup_rank
FROM deduped
ORDER BY dedup_rank DESC
