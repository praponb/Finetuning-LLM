SELECT a.id, a.opened_date, a.account_type
FROM accounts a
WHERE a.status IS NOT NULL AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id);
