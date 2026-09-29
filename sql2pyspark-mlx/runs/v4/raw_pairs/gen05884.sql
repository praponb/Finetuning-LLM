SELECT
    a.account_type,
    a.balance,
    a.opened_date,
    t.account_id
FROM accounts a
JOIN transactions t ON a.id = t.account_id
