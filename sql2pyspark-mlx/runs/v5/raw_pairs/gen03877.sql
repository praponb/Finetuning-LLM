SELECT HOUR(paid_at) AS paid_hour, SUM(amount) AS total_amount, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk
FROM payments
WHERE method = 'card'
GROUP BY HOUR(paid_at)
ORDER BY rnk DESC
