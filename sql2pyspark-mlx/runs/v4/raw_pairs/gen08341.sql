SELECT
    *,
    ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share,
    SUM(amount) OVER w_grp AS running_amount,
    NTILE(3) OVER w_ordered AS amount_bucket,
    SUM(amount) OVER w_grp AS running_amount_2,
    NTILE(3) OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS amount_bucket_2
FROM transactions
WINDOW w_grp AS (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY txn_type ORDER BY amount, id)
