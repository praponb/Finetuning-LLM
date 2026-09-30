SELECT
    t.id,
    t.txn_time,
    a.status,
    a.customer_id
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
ORDER BY 4 DESC;
