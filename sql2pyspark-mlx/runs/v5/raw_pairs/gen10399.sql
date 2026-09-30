SELECT a.status, a.customer_id, ROUND(STDDEV(t.amount), 2) AS stddev_amount
FROM transactions t
JOIN accounts a ON t.account_id = a.id
JOIN customers c ON a.customer_id = c.id
GROUP BY a.status, a.customer_id
ORDER BY customer_id DESC, a.status;
