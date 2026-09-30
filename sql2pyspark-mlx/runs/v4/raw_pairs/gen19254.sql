SELECT status FROM accounts WHERE status = 'CLOSED' UNION ALL SELECT status FROM subscriptions WHERE plan <> 'enterprise'
