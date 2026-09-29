WITH with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions_payments
),
sessionized AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        is_new_burst,
        burst_id,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    SELECT s.*, o.status
    FROM sessionized s
    LEFT JOIN orders o
        ON s.order_id <=> o.id
    WHERE o.shipping_fee <= 16
),
scored AS (
    SELECT
        *,
        SUM(session_id) OVER w_running last7_sum_session_id,
        AVG(session_id) OVER w_running last7_avg_session_id,
        MAX(session_id) OVER w_running last7_max_session_id
    FROM enriched
    WINDOW w_running AS (PARTITION BY status ORDER BY is_new_burst, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored_payments AS (
    SELECT
        *,
        LAG(is_new_burst) OVER (PARTITION BY order_id ORDER BY session_id, id) prev_is_new_burst,
        LEAD(is_new_burst) OVER (PARTITION BY order_id ORDER BY session_id, id) next_is_new_burst,
        LEAD(burst_id) OVER (PARTITION BY order_id ORDER BY session_id, id) next_burst_id,
        LAG(burst_id) OVER (PARTITION BY order_id ORDER BY session_id, id) prev_burst_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY session_id, id) seq_no
    FROM scored
)
SELECT
    p.status AS order_status,
    s.status AS payment_status,
    COUNT(*) AS num_rows,
    AVG(s.next_burst_id) AS avg_next_burst_id
FROM scored_payments s
LEFT JOIN orders p ON s.order_id = p.id
GROUP BY GROUPING SETS ((p.status, s.status), (p.status), ())
ORDER BY avg_next_burst_id DESC NULLS LAST;
