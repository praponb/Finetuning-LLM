SELECT
    *,
    AVG(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_quantity,
    COUNT(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_quantity,
    MAX(quantity) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_quantity,
    NTILE(3) OVER (ORDER BY quantity, id) AS quantity_bucket
FROM order_items
