SELECT
    id,
    product_id,
    quantity,
    MAX(quantity) OVER (PARTITION BY product_id) AS product_id_max_quantity,
    SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity
FROM order_items;
