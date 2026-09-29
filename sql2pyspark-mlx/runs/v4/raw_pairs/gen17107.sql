SELECT o.status, SUM(CASE WHEN p.status = 'FAILED' THEN p.amount ELSE 0 END) AS failed_amount FROM payments p LEFT JOIN orders o ON o.id = p.order_id GROUP BY o.status;
