SELECT a.id, a.opened_date, a.status
FROM accounts a
WHERE a.status <> 'CLOSED' AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
