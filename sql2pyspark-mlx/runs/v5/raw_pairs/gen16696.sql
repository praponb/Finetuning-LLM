SELECT
    id,
    order_id,
    quantity,
    product_id,
    unit_price,
    AVG(quantity) OVER (PARTITION BY order_id) AS order_id_avg_quantity,
    ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id, order_id), 3) AS unit_price_share
FROM order_items
