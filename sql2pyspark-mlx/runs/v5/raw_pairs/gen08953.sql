SELECT id, method, amount, order_id, PERCENT_RANK() OVER (PARTITION BY method ORDER BY amount) amount_pct_rank, MAX(amount) OVER (PARTITION BY order_id) order_id_max_amount FROM payments
