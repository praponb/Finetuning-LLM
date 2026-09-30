SELECT channel, AVG(amount) AS avg_amount, SUM(amount) AS total_amount
FROM orders
GROUP BY channel
HAVING avg_amount >= 570
ORDER BY 3
