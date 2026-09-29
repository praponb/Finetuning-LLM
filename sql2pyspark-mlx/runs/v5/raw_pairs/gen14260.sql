SELECT opened_date
FROM accounts
WHERE account_type IN ('LOAN', 'SAVINGS')
EXCEPT
SELECT start_date
FROM subscriptions
ORDER BY opened_date
