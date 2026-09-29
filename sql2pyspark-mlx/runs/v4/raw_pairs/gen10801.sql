SELECT
    *,
    RANK() OVER (PARTITION BY account_id, txn_type ORDER BY amount DESC NULLS FIRST) amount_rank,
    NTILE(10) OVER (ORDER BY amount ASC, id ASC) amount_bucket,
    AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) amount_avg_3
FROM transactions
