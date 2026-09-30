SELECT status
FROM accounts
WHERE opened_date > '2023-11-17'
UNION
SELECT status
FROM subscriptions
ORDER BY status
