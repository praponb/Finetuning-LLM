WITH with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_date
    FROM stores s
    JOIN LATERAL (
            SELECT c.order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft
FROM with_last
