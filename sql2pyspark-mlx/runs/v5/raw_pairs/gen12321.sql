SELECT
    id,
    txn_time,
    merchant,
    is_flagged,
    ROW_NUMBER() OVER w_recent AS row_seq,
    AVG(amount) OVER (PARTITION BY merchant) AS merchant_avg_amount
FROM transactions
WINDOW w_recent AS (PARTITION BY account_id ORDER BY txn_time, id);
