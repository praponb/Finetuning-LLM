WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount NULLS FIRST, id) AS dedup_rank
    FROM payments
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    method,
    amount,
    paid_at,
    dedup_rank
FROM deduped
ORDER BY dedup_rank DESC NULLS FIRST
