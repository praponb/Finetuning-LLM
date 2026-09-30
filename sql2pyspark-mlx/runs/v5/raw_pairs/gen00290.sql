SELECT
    t.id,
    t.txn_time,
    t.txn_type,
    a.opened_date,
    a.customer_id
FROM transactions t
RIGHT JOIN accounts a ON t.account_id = a.id
ORDER BY id DESC
