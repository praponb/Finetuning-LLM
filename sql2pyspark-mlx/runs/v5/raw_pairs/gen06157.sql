SELECT status
FROM accounts
UNION
SELECT status
FROM subscriptions
WHERE monthly_fee BETWEEN 90 AND 200;
