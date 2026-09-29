SELECT id, order_id, amount, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) rnk FROM payments;
