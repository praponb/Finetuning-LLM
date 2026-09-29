WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(method, 'UNKNOWN') AS method_filled
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-06-02 14:00:00')
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY order_id, status ORDER BY amount DESC) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status, method ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        SUM(amount) OVER (PARTITION BY status, method ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        COUNT(amount) OVER (PARTITION BY status, method ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM latest
),
with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
ranked AS (
    SELECT *
    FROM sessions_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY session_start_flag NULLS LAST, id) = 1
)
SELECT
    p.status AS order_status,
    a.status AS payment_status,
    COUNT(*) AS cnt,
    AVG(a.running_count_amount) AS avg_running_count_amount,
    ROUND(SUM(a.session_no), 2) AS total_session_no,
    GROUPING_ID() AS level_id
FROM ranked a
LEFT JOIN orders p ON a.order_id = p.id
GROUP BY CUBE (p.status, a.status)
ORDER BY total_session_no DESC
