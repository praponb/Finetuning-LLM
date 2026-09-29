WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        item.product_id,
        item.qty,
        item.price
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS item
)
SELECT
    id,
    customer_id,
    created_at,
    product_id,
    qty
FROM exploded;
