SELECT customer_id
FROM accounts
WHERE status IN ('CLOSED', 'FROZEN')
UNION
SELECT customer_id
FROM accounts
WHERE account_type = 'CREDIT'
ORDER BY customer_id
