SELECT order_id, SUM(unit_price * quantity) AS revenue, COUNT(unit_price) AS unit_price_count FROM order_items WHERE product_id = 30 GROUP BY 1 HAVING unit_price_count >= 3 ORDER BY order_id
