SELECT a.id, a.status
FROM accounts a
WHERE a.opened_date < '2024-08-01' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
