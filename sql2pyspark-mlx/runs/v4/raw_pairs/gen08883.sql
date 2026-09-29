SELECT t.id, t.is_flagged
FROM transactions t
WHERE t.account_id IN (SELECT id FROM accounts WHERE customer_id IS NOT NULL)
