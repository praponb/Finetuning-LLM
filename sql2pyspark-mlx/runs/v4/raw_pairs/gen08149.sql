SELECT
    id,
    product_id,
    order_id,
    unit_price,
    COUNT(*) OVER (PARTITION BY product_id) AS product_id_count,
    SUM(unit_price) OVER (PARTITION BY order_id ORDER BY unit_price, id) AS running_unit_price
FROM order_items
ORDER BY id DESC;
