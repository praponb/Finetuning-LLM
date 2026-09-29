SELECT status, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
WHERE order_id IS NULL
GROUP BY status;
