SELECT
    id,
    quantity,
    COUNT(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_unit_price,
    MIN(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_unit_price,
    MAX(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_unit_price,
    SUM(unit_price) OVER (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_unit_price
FROM order_items
