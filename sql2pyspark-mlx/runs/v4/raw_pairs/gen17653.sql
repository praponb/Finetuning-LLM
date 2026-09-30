SELECT
    id,
    elem.product_id,
    elem.qty,
    elem.price
FROM (
    SELECT id, EXPLODE(items) AS elem
    FROM carts
) t;
