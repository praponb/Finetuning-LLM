SELECT
    id,
    account_id,
    txn_time,
    txn_type,
    amount,
    RANK() OVER (PARTITION BY account_id ORDER BY txn_time) rnk,
    MAX(amount) OVER (PARTITION BY txn_type) txn_type_max_amount
FROM transactions
