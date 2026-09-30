SELECT t.id, t.amount
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE balance IS NOT NULL)
