WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS dedup_rank
    FROM stores
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    dedup_rank
FROM ranked
