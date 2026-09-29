SELECT t.id, t.amount
FROM transactions t
WHERE t.account_id IN (6, 23) AND t.account_id IN (SELECT id FROM accounts WHERE status IN ('FROZEN', 'OPEN'))
