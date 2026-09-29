SELECT p.id, p.launch_date, t.avg_unit_price FROM products p LEFT OUTER JOIN (SELECT product_id, AVG(unit_price) AS avg_unit_price FROM order_items GROUP BY product_id) t ON p.id = t.product_id
