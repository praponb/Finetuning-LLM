WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(amount, 2, 0) OVER (PARTITION BY method, order_id ORDER BY paid_at, id) AS next_amount,
        SUM(amount) OVER (PARTITION BY status, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS row_seq
    FROM payments
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        paid_at,
        status,
        rn,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY running_amount DESC, id) AS seq_no,
        CUME_DIST() OVER (ORDER BY row_seq DESC NULLS LAST) AS row_seq_cume_dist,
        MAX(next_amount) OVER (PARTITION BY order_id) AS order_id_max_next_amount
    FROM latest
    WHERE next_amount IS NULL
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM with_windows_payments
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM sessions_payments
    INTERSECT
    SELECT status
    FROM payments
    WHERE method IN ('card', 'cash', 'promptpay')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_payments s
    INNER JOIN remaining_keys k ON s.status = k.status
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        paid_at,
        rn,
        seq_no,
        row_seq_cume_dist,
        order_id_max_next_amount,
        session_no,
        order_id_max_next_amount * session_no AS order_id_max_next_amount_times_session_no,
        session_no + rn AS session_no_plus_rn
    FROM verified
    WHERE method IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        paid_at,
        rn,
        seq_no,
        row_seq_cume_dist,
        order_id_max_next_amount,
        session_no,
        order_id_max_next_amount_times_session_no,
        row_seq_cume_dist * session_no_plus_rn AS row_seq_cume_dist_times_session_no_plus_rn
    FROM base
    WHERE paid_at >= DATE '2023-06-13'
      AND method IS NOT DISTINCT FROM 'card'
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN order_id_max_next_amount >= 1236 THEN 'LARGE' ELSE 'SMALL' END AS order_id_max_next_amount_tier,
        ROUND(SUM(session_no) / NULLIF(COUNT(CASE WHEN method = 'cash' THEN 1 END), 0), 2) AS session_no_per_cash,
        SIZE(COLLECT_SET(method)) AS num_methods,
        GROUPING(CASE WHEN order_id_max_next_amount >= 1236 THEN 'LARGE' ELSE 'SMALL' END) AS gid
    FROM cleaned
    GROUP BY ROLLUP (CASE WHEN order_id_max_next_amount >= 1236 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    order_id_max_next_amount_tier,
    session_no_per_cash,
    gid
FROM stats
ORDER BY order_id_max_next_amount_tier NULLS LAST, gid;
