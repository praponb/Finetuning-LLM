SELECT status, SUM(amount) total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM payments WHERE order_id IN (12, 39) GROUP BY status
