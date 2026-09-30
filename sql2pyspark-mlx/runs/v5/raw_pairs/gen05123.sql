SELECT p.order_id, p.amount, p.paid_at, o.channel, o.store_id FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id;
