SELECT o.status, SUM(p.amount) total_amount FROM payments p LEFT JOIN orders o ON p.order_id = o.id GROUP BY o.status HAVING SUM(p.amount) >= 2600 ORDER BY status;
