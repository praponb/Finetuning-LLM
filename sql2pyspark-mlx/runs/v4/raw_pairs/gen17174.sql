SELECT order_id, AVG(amount) AS avg_amount, MIN(amount) AS min_amount
FROM payments
GROUP BY order_id
ORDER BY 1 DESC;
