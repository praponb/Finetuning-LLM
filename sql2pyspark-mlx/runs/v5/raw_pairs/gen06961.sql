WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.rating,
        top1.category
    FROM suppliers s
    JOIN LATERAL (
            SELECT c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT id, category
FROM with_last
ORDER BY category DESC
