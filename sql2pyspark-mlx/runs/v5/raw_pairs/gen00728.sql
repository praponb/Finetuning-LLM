SELECT
    a.id,
    a.customer_id,
    a.opened_date,
    a.account_type
FROM accounts a
WHERE a.id IN (SELECT account_id FROM transactions)
ORDER BY id DESC
LIMIT 1
