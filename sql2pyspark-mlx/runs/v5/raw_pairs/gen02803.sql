WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        YEAR(txn_time) AS txn_time_year,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM transactions
    WHERE NOT is_flagged
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(burst_id) OVER w_recent AS last7_max_burst_id,
        MIN(burst_id) OVER w_recent AS last7_min_burst_id,
        SUM(burst_id) OVER w_recent AS last7_sum_burst_id,
        LEAD(new_group_flag, 2) OVER (ORDER BY burst_id, id) AS next_new_group_flag
    FROM sessions_transactions
    WINDOW w_recent AS (PARTITION BY account_id, merchant ORDER BY burst_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.balance,
        p.account_type
    FROM scored e
    LEFT JOIN accounts p
        ON e.account_id = p.id
    WHERE p.customer_id IS NULL
)
SELECT
    id,
    amount,
    is_flagged,
    txn_time_year,
    new_group_flag,
    burst_id,
    last7_min_burst_id,
    next_new_group_flag,
    balance
FROM joined
ORDER BY txn_time_year DESC, next_new_group_flag
