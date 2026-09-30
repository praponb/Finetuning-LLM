SELECT p.id, p.amount, p.paid_at, o.status, o.channel, o.store_id FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id WHERE o.shipping_fee BETWEEN 23 AND 27 AND o.customer_id IN (10, 15)
