SELECT
    a.id,
    a.opened_date,
    a.balance,
    a.status
FROM accounts a
WHERE EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.amount < 3300);
