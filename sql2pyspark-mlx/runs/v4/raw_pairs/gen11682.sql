SELECT product_id, SUM(unit_price) AS total_unit_price, MAX(unit_price) - MIN(unit_price) AS unit_price_range
FROM order_items
WHERE product_id IN (17, 24, 26, 36) AND product_id IS NULL
GROUP BY product_id
