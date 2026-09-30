SELECT c.segment, CASE WHEN c.credit_limit >= 14900 THEN 'high' WHEN c.credit_limit >= 8200 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, ROUND(STDDEV(t.amount), 2) AS stddev_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.status IN ('FROZEN', 'CLOSED')
GROUP BY c.segment, CASE WHEN c.credit_limit >= 14900 THEN 'high' WHEN c.credit_limit >= 8200 THEN 'mid' ELSE 'low' END;
