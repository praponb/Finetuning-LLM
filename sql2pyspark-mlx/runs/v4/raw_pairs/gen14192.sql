SELECT
    t.id,
    t.amount,
    t.merchant,
    t.txn_time,
    a.opened_date,
    a.customer_id
FROM transactions t
LEFT OUTER JOIN accounts a ON a.id = t.account_id
ORDER BY a.customer_id DESC, opened_date;
