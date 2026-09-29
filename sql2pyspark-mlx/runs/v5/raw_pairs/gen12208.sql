SELECT status, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM orders
WHERE discount BETWEEN 0.3 AND 0.31
GROUP BY status;
