SELECT
    id,
    product_id,
    quantity,
    order_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) rn
    FROM order_items
    WHERE order_id IN (7, 38)
) r
WHERE rn = 1;
