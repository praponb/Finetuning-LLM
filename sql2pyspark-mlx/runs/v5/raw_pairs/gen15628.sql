SELECT
    id,
    store_id,
    order_date,
    discount
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC NULLS FIRST, id) AS dedup_rank
    FROM orders
) ranked
WHERE dedup_rank = 1
