WITH base AS (
    SELECT
        id,
        amount,
        paid_at,
        status,
        CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded,
        CONCAT_WS('-', status, method) AS status_method_key,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM payments
    WHERE paid_at >= DATE '2023-07-11'
      AND method IS DISTINCT FROM 'card'
),
enriched AS (
    SELECT
        *,
        LAG(is_refunded) OVER w_recent AS prev_is_refunded,
        LAG(amount) OVER w_recent AS prev_amount,
        LEAD(amount) OVER w_recent AS next_amount,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM base
    WINDOW w_recent AS (PARTITION BY status ORDER BY is_refunded, id)
),
metrics AS (
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MIN(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        MAX(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        COUNT(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY status) AS status_stddev_samp_seq_no
    FROM enriched
    WHERE TO_DATE(paid_at) >= '2025-02-01'
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM metrics
),
sessions_payments AS (
    SELECT
        id,
        paid_at,
        status,
        is_refunded,
        resolved_method,
        prev_is_refunded,
        prev_amount,
        next_amount,
        seq_no,
        running_avg_amount,
        SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
base_payments AS (
    SELECT *, FLOOR(session_no) AS session_no_whole
    FROM sessions_payments
    WHERE paid_at > TO_DATE('2023-10-24')
)
SELECT
    CASE WHEN session_no_whole >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_no_whole_tier,
    DATE_TRUNC('MONTH', paid_at) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(prev_amount, 0.9) AS prev_amount_p90,
    SUM(prev_is_refunded) AS total_prev_is_refunded,
    GROUPING(CASE WHEN session_no_whole >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM base_payments
GROUP BY ROLLUP (CASE WHEN session_no_whole >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', paid_at))
