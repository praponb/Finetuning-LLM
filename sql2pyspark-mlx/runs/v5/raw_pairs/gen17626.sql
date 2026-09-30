SELECT order_id, SUM(unit_price) total_unit_price FROM order_items WHERE unit_price <= 370 OR product_id = 31 GROUP BY order_id HAVING SUM(unit_price) > 1300
