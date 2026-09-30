SELECT customer_id, status
FROM accounts
WHERE account_type IN ('CHECKING', 'CREDIT', 'SAVINGS')
UNION
SELECT customer_id, status
FROM accounts
WHERE account_type IN ('LOAN', 'CREDIT')
ORDER BY customer_id DESC;
