SELECT o.channel, e.status AS payment_status, COUNT(*) AS num_rows, MIN(e.amount) AS min_amount FROM payments e INNER JOIN orders o ON e.order_id = o.id GROUP BY o.channel, e.status ORDER BY channel
