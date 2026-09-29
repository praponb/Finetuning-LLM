SELECT id, order_id, amount, MAX(amount) OVER (PARTITION BY order_id) AS order_id_max_amount FROM payments;
