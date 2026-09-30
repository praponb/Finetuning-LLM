SELECT order_id, MAX(unit_price) - MIN(unit_price) AS unit_price_range, MAX(quantity) AS max_quantity
FROM order_items
GROUP BY order_id
ORDER BY order_id DESC
LIMIT 5
