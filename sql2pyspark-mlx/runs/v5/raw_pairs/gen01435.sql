SELECT order_id, SUM(unit_price) total_unit_price, MAX(quantity) max_quantity FROM order_items WHERE quantity IS NOT NULL OR unit_price < 340 GROUP BY 1
