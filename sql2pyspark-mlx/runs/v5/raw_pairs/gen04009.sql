SELECT p.brand, MIN(oi.unit_price) AS min_unit_price, SUM(oi.unit_price) AS total_unit_price
FROM order_items oi
JOIN products p ON p.id = oi.product_id
GROUP BY p.brand
ORDER BY brand DESC
