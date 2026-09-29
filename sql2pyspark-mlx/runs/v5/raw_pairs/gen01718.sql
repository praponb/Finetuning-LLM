WITH metrics AS (
    SELECT
        *,
        CUME_DIST() OVER w_grp AS amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, id) AS row_seq
    FROM payments
    WINDOW w_grp AS (ORDER BY amount NULLS LAST)
),
metrics_payments AS (
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY method ORDER BY amount_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount,
        MAX(amount) OVER (PARTITION BY method ORDER BY amount_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount,
        MIN(amount) OVER (PARTITION BY method ORDER BY amount_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount,
        AVG(amount) OVER (PARTITION BY method ORDER BY amount_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount
    FROM metrics
),
gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM metrics_payments
),
islands AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        amount_cume_dist,
        event_seq,
        row_seq,
        last7_count_amount,
        last7_max_amount,
        last7_avg_amount,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
gaps_2 AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessionized AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        row_seq,
        last7_avg_amount,
        session_no,
        SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM gaps_2
),
flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
islands_payments AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_3
    FROM flagged
),
with_windows AS (
    SELECT
        id,
        amount,
        paid_at,
        last7_avg_amount,
        session_no_2,
        is_new_session,
        session_no_3,
        AVG(session_no) OVER w_ordered AS running_avg_session_no,
        COUNT(session_no) OVER w_ordered AS running_count_session_no,
        SUM(session_no) OVER w_ordered AS running_sum_session_no
    FROM islands_payments
    WINDOW w_ordered AS (PARTITION BY method ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, ROUND(is_new_session * 0.9, 2) AS adjusted_is_new_session
    FROM with_windows
    WHERE TO_DATE(paid_at) < '2023-10-01'
)
SELECT
    CASE WHEN running_count_session_no >= 15 THEN 'LARGE' ELSE 'SMALL' END AS running_count_session_no_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY 1;
