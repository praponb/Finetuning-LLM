SELECT account_type
FROM accounts
WHERE opened_date BETWEEN '2023-05-07' AND '2024-07-01'
INTERSECT
SELECT account_type
FROM accounts
WHERE account_type = 'CREDIT';
