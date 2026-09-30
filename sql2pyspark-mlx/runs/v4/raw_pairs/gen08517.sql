WITH exploded AS (
    SELECT
        id,
        created_at,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) li AS line
)
SELECT
    created_at,
    product_id,
    qty,
    price
FROM exploded
ORDER BY product_id DESC, qty DESC;
