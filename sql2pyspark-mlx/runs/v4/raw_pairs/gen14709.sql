SELECT
    t.id,
    t.txn_time,
    a.opened_date,
    c.segment
FROM transactions t
JOIN accounts a ON t.account_id = a.id
FULL OUTER JOIN customers c ON c.id = a.customer_id
WHERE a.status IN ('FROZEN', 'CLOSED', 'OPEN')
