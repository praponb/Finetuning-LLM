SELECT o.store_id, COUNT(p.amount) AS amount_count FROM payments p LEFT JOIN orders o ON p.order_id = o.id WHERE p.amount < 610 GROUP BY o.store_id;
