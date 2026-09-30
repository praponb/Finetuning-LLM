SELECT p.channel, COUNT(*) AS num_rows, SUM(s.quantity) AS total_quantity, AVG(s.unit_price) AS avg_unit_price FROM order_items s JOIN orders p ON s.order_id = p.id GROUP BY CUBE (p.channel)
