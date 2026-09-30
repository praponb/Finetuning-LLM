SELECT t.id, t.amount, t.is_flagged
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE status <> 'OPEN');
