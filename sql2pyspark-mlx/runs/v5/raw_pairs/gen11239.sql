SELECT
    id,
    account_id,
    txn_time,
    amount,
    txn_type,
    SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS amount_rank
FROM transactions;
