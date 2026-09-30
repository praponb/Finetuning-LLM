SELECT product_id, COUNT(*) AS row_count, MAX(unit_price) AS max_unit_price
FROM order_items
GROUP BY 1
HAVING SUM(quantity * unit_price) < 57
ORDER BY row_count DESC, product_id
LIMIT 10
