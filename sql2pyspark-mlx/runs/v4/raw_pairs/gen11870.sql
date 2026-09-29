SELECT opened_date AS event_date, 'account' AS source
FROM accounts
WHERE account_type NOT IN ('CHECKING', 'CREDIT')
UNION
SELECT start_date AS event_date, 'subscription' AS source
FROM subscriptions
WHERE plan = 'standard'
ORDER BY source DESC
