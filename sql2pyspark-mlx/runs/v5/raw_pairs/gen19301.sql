SELECT product_id, COUNT(unit_price) AS unit_price_count, MAX(quantity) - MIN(quantity) AS quantity_range
FROM order_items
WHERE unit_price > 160 OR unit_price >= 250
GROUP BY product_id
