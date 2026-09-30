SELECT status, account_type
FROM accounts
EXCEPT DISTINCT
SELECT status, account_type
FROM accounts
WHERE opened_date < '2023-10-01';
