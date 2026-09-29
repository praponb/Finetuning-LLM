SELECT id, opened_date
FROM accounts
WHERE opened_date BETWEEN '2023-11-01' AND '2024-04-01'
UNION ALL
SELECT id, opened_date
FROM accounts
WHERE customer_id IS NULL
