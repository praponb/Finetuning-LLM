SELECT status
FROM accounts
UNION
SELECT status
FROM subscriptions
WHERE monthly_fee < 190;
