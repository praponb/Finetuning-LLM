SELECT
    *,
    SUM(amount) OVER w_running AS running_sum_amount,
    MAX(amount) OVER w_running AS running_max_amount,
    MIN(amount) OVER w_running AS running_min_amount,
    LEAD(amount, 1) OVER (PARTITION BY merchant ORDER BY amount, id) AS next_amount
FROM transactions
WHERE txn_type IN ('FEE', 'CREDIT')
WINDOW w_running AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
