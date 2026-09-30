SELECT a.status, a.customer_id, ROUND(AVG(t.amount), 2) AS avg_amount
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
GROUP BY a.status, a.customer_id
ORDER BY customer_id
