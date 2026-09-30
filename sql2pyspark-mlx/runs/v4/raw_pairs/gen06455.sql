SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    AVG(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_avg_7d,
    AVG(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
    ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) AS event_seq,
    AVG(amount) OVER (PARTITION BY account_id, merchant ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
    MIN(amount) OVER w_part AS merchant_min_amount
FROM transactions
WINDOW w_part AS (PARTITION BY merchant);
