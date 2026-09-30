SELECT order_id, SUM(quantity) AS total_quantity, COUNT(quantity) AS quantity_count
FROM order_items
WHERE unit_price BETWEEN 190 AND 220 AND quantity BETWEEN 3 AND 6
GROUP BY order_id
ORDER BY quantity_count DESC;
