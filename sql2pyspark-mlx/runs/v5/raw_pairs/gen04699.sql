SELECT o.channel, COUNT(*) AS payment_count FROM payments p LEFT JOIN orders o ON o.id = p.order_id GROUP BY o.channel ORDER BY channel LIMIT 1
