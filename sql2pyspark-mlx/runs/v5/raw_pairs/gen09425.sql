WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY account_id, merchant ORDER BY is_new_burst DESC NULLS LAST) AS rank_in_group
    FROM sessionized
    QUALIFY rank_in_group = 1
),
with_dims AS (
    -- Attach accounts attributes
    SELECT src.*, b.status
    FROM latest src
    JOIN accounts b
        ON src.account_id = b.id
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) AS prev_txn_time
    FROM with_dims
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, status ORDER BY prev_txn_time DESC, id) AS row_num
    FROM sessions_transactions
    QUALIFY row_num = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(status, TRUE) OVER (PARTITION BY status ORDER BY session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
        NTILE(5) OVER (PARTITION BY account_id ORDER BY is_new_burst DESC, id) AS is_new_burst_bucket,
        NTILE(10) OVER (ORDER BY session_no DESC, id) AS session_no_bucket,
        AVG(new_group_flag) OVER (PARTITION BY txn_type ORDER BY CAST(prev_txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS new_group_flag_avg_90d,
        NTILE(4) OVER (ORDER BY amount DESC, id) AS amount_bucket
    FROM ranked
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN prev_txn_time - LAG(prev_txn_time) OVER (PARTITION BY account_id ORDER BY prev_txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY account_id ORDER BY prev_txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time,
    merchant,
    is_flagged,
    is_new_burst,
    session_no,
    status
FROM islands
ORDER BY txn_time DESC NULLS FIRST
