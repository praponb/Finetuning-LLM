SELECT o.store_id, MAX(p.paid_at) last_paid_at FROM payments p LEFT JOIN orders o ON p.order_id = o.id GROUP BY o.store_id ORDER BY store_id DESC, 2 ASC LIMIT 1;
