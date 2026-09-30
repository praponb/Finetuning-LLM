WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM transactions
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        SUM(session_start_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        island_id,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant
    FROM sessions_transactions
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LEAST(island_id, amount) AS least_island_id_amount
    FROM metrics
    WHERE txn_time > DATE '2023-06-08'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        amount,
        txn_time,
        merchant,
        island_id,
        amount_avg_5,
        first_known_merchant,
        least_island_id_amount,
        DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label
    FROM base
    WHERE merchant IS NOT NULL
      AND island_id > 5
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(island_id) OVER w_hist AS last3_sum_island_id,
        COUNT(island_id) OVER w_hist AS last3_count_island_id,
        AVG(island_id) OVER w_hist AS last3_avg_island_id,
        RANK() OVER (PARTITION BY merchant ORDER BY island_id DESC) AS island_id_rank
    FROM filtered
    WHERE txn_time >= '2025-07-01'
    WINDOW w_hist AS (PARTITION BY merchant ORDER BY amount_avg_5, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    session_id,
    SUM(CASE WHEN merchant = 'Grab' THEN new_group_flag ELSE 0 END) AS grab_new_group_flag,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM sessionized
GROUP BY DATE_TRUNC('MONTH', txn_time), session_id
ORDER BY grab_new_group_flag;
