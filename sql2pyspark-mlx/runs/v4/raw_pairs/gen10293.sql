SELECT
    a.opened_date,
    a.account_type,
    a.status,
    t.is_flagged,
    t.account_id,
    t.merchant
FROM accounts a
INNER JOIN transactions t ON t.account_id = a.id
WHERE t.merchant = '7-Eleven';
