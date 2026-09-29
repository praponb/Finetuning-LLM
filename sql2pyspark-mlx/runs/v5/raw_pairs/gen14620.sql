SELECT
    id,
    unit_price,
    order_id,
    product_id
FROM order_items
ORDER BY product_id ASC;
