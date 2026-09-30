SELECT p.id, p.paid_at, o.store_id, o.amount, o.order_date FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id WHERE p.order_id = 10 ORDER BY paid_at ASC, 1 DESC
