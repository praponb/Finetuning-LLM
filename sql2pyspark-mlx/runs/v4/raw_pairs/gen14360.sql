SELECT order_id, SUM(unit_price) total_unit_price, RANK() OVER (ORDER BY SUM(unit_price) DESC) AS unit_price_rank FROM order_items GROUP BY order_id;
