SELECT id, opened_date
FROM accounts
WHERE account_type = 'CREDIT'
UNION ALL
SELECT id, opened_date
FROM accounts
WHERE customer_id = 10
ORDER BY opened_date;
