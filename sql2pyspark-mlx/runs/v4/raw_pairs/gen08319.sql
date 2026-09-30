SELECT
    a.opened_date,
    a.status,
    a.balance,
    t.is_flagged,
    t.amount,
    t.txn_time
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
WHERE a.customer_id IN (2, 7, 30, 32);
