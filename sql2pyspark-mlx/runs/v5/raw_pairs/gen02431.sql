WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST, id) AS dedup_rank
        FROM payments
    ) t
    WHERE dedup_rank = 1
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) AS prev_paid_at
    FROM ranked
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY paid_at, id) AS row_seq,
        MAX(session_id) OVER (PARTITION BY order_id, status) AS order_id_max_session_id
    FROM sessions_payments
    WHERE session_id != 6
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY order_id_max_session_id DESC NULLS FIRST, id) AS rank_in_group
        FROM enriched
    ) ranked
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        paid_at,
        session_id,
        row_seq,
        order_id_max_session_id,
        rank_in_group,
        LAG(session_id) OVER w_grp AS prev_session_id,
        LAG(row_seq) OVER w_grp AS prev_row_seq,
        ROW_NUMBER() OVER w_grp AS row_seq_2
    FROM deduped
    WINDOW w_grp AS (PARTITION BY method ORDER BY session_id, id)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        session_id,
        row_seq,
        order_id_max_session_id,
        prev_row_seq,
        SUM(order_id_max_session_id) OVER (PARTITION BY order_id ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_id_max_session_id,
        LAG(prev_session_id, 1, 0) OVER (PARTITION BY order_id ORDER BY row_seq_2, id) AS prev_prev_session_id
    FROM with_windows
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        order_id_max_session_id,
        prev_row_seq,
        running_order_id_max_session_id,
        LAG(running_order_id_max_session_id) OVER w_hist AS prev_running_order_id_max_session_id,
        LEAD(row_seq) OVER w_hist AS next_row_seq,
        ROW_NUMBER() OVER w_hist AS row_seq_2
    FROM scored
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY order_id_max_session_id, id)
)
-- Aggregate with conditional metrics
SELECT
    running_order_id_max_session_id,
    prev_running_order_id_max_session_id,
    CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_2_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(prev_row_seq, 0.5) AS prev_row_seq_p50,
    COUNT(DISTINCT id) AS distinct_ids_2,
    GROUPING(running_order_id_max_session_id) AS gid
FROM metrics
GROUP BY ROLLUP (running_order_id_max_session_id, prev_running_order_id_max_session_id, CASE WHEN row_seq_2 >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY distinct_ids DESC NULLS FIRST;
