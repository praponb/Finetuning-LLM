SELECT
    a.id,
    a.status,
    a.account_type,
    a.opened_date
FROM accounts a
WHERE a.status <> 'CLOSED' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount IS NOT NULL);
