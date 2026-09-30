SELECT
    id,
    order_id,
    product_id,
    quantity,
    ROUND(quantity / SUM(quantity) OVER (PARTITION BY order_id, product_id), 3) AS quantity_share
FROM order_items;
