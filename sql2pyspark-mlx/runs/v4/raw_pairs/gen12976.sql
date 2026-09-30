SELECT
    *,
    LAG(amount, 1) OVER (ORDER BY txn_time, id) AS prev_amount,
    NTILE(4) OVER w_ordered AS amount_bucket,
    ROW_NUMBER() OVER w_ordered_2 AS event_seq
FROM transactions
WINDOW w_ordered AS (PARTITION BY txn_type ORDER BY amount, id), w_ordered_2 AS (ORDER BY amount DESC, id)
