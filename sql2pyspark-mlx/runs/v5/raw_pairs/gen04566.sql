SELECT
    id,
    order_id,
    unit_price,
    product_id
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn
    FROM order_items
) AS r
WHERE rn <= 3;
