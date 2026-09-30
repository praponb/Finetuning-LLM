SELECT status, MIN(balance) AS min_balance
FROM accounts
WHERE status = 'FROZEN' AND status != 'OPEN'
GROUP BY status
