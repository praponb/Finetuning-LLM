SELECT
    t.id,
    t.txn_time,
    t.amount,
    t.is_flagged,
    a.opened_date,
    a.customer_id,
    a.account_type
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
WHERE a.account_type = 'CHECKING';
