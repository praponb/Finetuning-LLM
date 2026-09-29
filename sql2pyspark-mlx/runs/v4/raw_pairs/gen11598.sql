SELECT order_id, SUM(unit_price) total_unit_price, RANK() OVER (ORDER BY SUM(unit_price) DESC) AS unit_price_rank FROM order_items WHERE order_id IN (16, 26, 35) GROUP BY order_id ORDER BY order_id
