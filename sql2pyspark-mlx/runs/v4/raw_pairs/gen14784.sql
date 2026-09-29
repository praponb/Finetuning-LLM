SELECT o.status, MAX(p.paid_at) AS latest_paid_at, MIN(p.paid_at) AS min_paid_at FROM payments p LEFT JOIN orders o ON p.order_id = o.id GROUP BY o.status ORDER BY o.status;
