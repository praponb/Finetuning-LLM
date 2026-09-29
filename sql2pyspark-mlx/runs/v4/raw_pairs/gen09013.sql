SELECT id, opened_date
FROM accounts
WHERE customer_id IS NOT NULL
UNION
SELECT id, opened_date
FROM accounts
WHERE balance >= 63000;
