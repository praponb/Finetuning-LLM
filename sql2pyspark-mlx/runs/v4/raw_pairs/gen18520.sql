SELECT
    t.id,
    t.txn_time,
    t.merchant,
    a.status,
    a.balance,
    a.customer_id
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
WHERE t.merchant <> 'Starbucks' AND t.is_flagged;
