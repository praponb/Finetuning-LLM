SELECT CASE WHEN amount >= 3000 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END AS amount_bucket, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank
FROM transactions
GROUP BY CASE WHEN amount >= 3000 THEN 'high' WHEN amount >= 1500 THEN 'mid' ELSE 'low' END
ORDER BY amount_bucket, amount_rank DESC
