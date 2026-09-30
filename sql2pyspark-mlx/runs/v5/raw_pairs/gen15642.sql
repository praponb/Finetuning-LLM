SELECT status
FROM accounts
WHERE balance > 61000
EXCEPT
SELECT status
FROM subscriptions;
