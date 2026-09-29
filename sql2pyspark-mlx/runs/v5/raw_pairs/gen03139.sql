SELECT o.customer_id, MIN(p.amount) min_amount, MIN(p.paid_at) first_paid_at
FROM payments p
FULL OUTER JOIN orders o ON p.order_id = o.id
GROUP BY o.customer_id;
