SELECT
    t.id,
    t.is_flagged,
    t.account_id,
    t.amount,
    a.customer_id,
    a.balance,
    a.account_type
FROM transactions t
FULL OUTER JOIN accounts a ON t.account_id = a.id
WHERE a.account_type = 'LOAN' OR HOUR(t.txn_time) BETWEEN 10 AND 17
