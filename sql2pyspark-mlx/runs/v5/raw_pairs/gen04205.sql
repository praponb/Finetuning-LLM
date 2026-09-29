SELECT
    id,
    product_id,
    unit_price,
    order_id,
    quantity,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
    MAX(quantity) OVER (PARTITION BY order_id) AS order_id_max_quantity
FROM order_items
ORDER BY order_id
