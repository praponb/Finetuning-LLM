SELECT customer_id
FROM accounts
WHERE balance <= 25000
UNION ALL
SELECT customer_id
FROM accounts
WHERE status IS NULL;
