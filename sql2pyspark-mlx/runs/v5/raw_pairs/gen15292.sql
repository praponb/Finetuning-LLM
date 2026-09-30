SELECT d.channel, d.status AS order_status, COUNT(*) AS num_rows, AVG(src.amount) AS avg_amount FROM payments src LEFT JOIN orders d ON src.order_id = d.id GROUP BY d.channel, d.status
