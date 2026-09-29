SELECT
    *,
    SUM(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
    SUM(amount) OVER w_ordered AS cumulative_amount,
    ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time DESC, id) AS event_seq,
    NTILE(5) OVER (PARTITION BY txn_type ORDER BY amount, id) AS amount_bucket
FROM transactions
WINDOW w_ordered AS (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
