SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    NTILE(10) OVER (PARTITION BY txn_type, merchant ORDER BY amount DESC, id) AS amount_bucket,
    RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC) AS amount_rank,
    SUM(amount) OVER (PARTITION BY account_id, merchant) AS account_id_sum_amount,
    ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq,
    NTILE(3) OVER (PARTITION BY txn_type, account_id ORDER BY amount, id) AS amount_bucket_2
FROM transactions;
