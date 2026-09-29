SELECT
    id,
    txn_type,
    txn_time,
    is_flagged,
    AVG(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
    SUM(amount) OVER w_grp AS cumulative_amount
FROM transactions
WINDOW w_grp AS (PARTITION BY account_id, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
