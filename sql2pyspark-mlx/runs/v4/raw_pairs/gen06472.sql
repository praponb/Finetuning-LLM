SELECT a.id, a.status, a.account_type
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.account_id = 40)
ORDER BY account_type ASC, id ASC
LIMIT 5
