SELECT
    t.txn_time,
    t.amount,
    a.status,
    a.customer_id
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
WHERE a.opened_date <= '2025-09-01'
ORDER BY a.customer_id ASC
