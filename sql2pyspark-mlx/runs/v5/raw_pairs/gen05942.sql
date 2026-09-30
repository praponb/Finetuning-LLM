SELECT a.id, a.status, a.balance
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount > 1900)
