WITH with_last AS (
    SELECT s.*, latest.quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    brand,
    price,
    cost,
    stock_qty,
    supplier_id,
    quantity
FROM with_last
ORDER BY price DESC, quantity DESC;
