SELECT p.id, p.amount, p.method, p.paid_at, o.customer_id, CASE WHEN o.channel = 'partner' THEN 1 ELSE 0 END AS is_partner FROM payments p INNER JOIN orders o ON p.order_id = o.id
