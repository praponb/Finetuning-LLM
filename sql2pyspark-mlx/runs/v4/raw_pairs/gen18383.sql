SELECT
    id,
    account_id,
    txn_time,
    RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS rnk
FROM transactions
WHERE merchant = 'Grab'
