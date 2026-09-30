SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    DENSE_RANK() OVER w_part AS amount_dense_rank,
    SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
FROM transactions
WINDOW w_part AS (PARTITION BY account_id, txn_type ORDER BY amount DESC NULLS LAST);
