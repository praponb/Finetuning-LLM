WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-06-21 02:00:00'
      AND merchant IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(amount) OVER w_part AS last4_count_amount,
        AVG(amount) OVER w_part AS last4_avg_amount,
        MAX(amount) OVER w_part AS last4_max_amount
    FROM base
    WHERE txn_type = 'FEE'
    WINDOW w_part AS (PARTITION BY merchant ORDER BY txn_time_period, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.*,
        b.status,
        b.account_type
    FROM metrics a
    INNER JOIN accounts b
        ON a.account_id = b.id
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time_period) OVER (PARTITION BY merchant ORDER BY txn_time_period, id) AS prev_txn_time_period
    FROM with_dims
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time_period IS NULL OR txn_time_period > prev_txn_time_period + INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        merchant,
        txn_time_period,
        last4_count_amount,
        last4_avg_amount,
        account_type,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        merchant,
        account_type,
        last4_avg_amount
    FROM islands
) src
PIVOT (
    AVG(last4_avg_amount)
    FOR account_type IN ('CHECKING', 'LOAN', 'CREDIT')
);
