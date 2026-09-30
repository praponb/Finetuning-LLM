SELECT
    a.id,
    a.status,
    a.account_type,
    a.opened_date
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions WHERE amount >= 1200);
