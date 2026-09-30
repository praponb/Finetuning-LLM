SELECT method, SUM(amount) total_amount, COUNT(*) cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM payments GROUP BY method ORDER BY cnt DESC;
