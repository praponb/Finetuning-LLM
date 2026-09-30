WITH metrics AS (
    SELECT
        *,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS amount_dense_rank,
        MIN(amount) OVER (PARTITION BY method) AS method_min_amount,
        LEAD(amount, 1) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_amount,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM payments
    WINDOW w_hist AS (ORDER BY amount, id)
),
prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM metrics
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
joined AS (
    SELECT
        src.*,
        b.discount,
        b.channel,
        b.amount AS order_amount
    FROM islands src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
scored AS (
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY method ORDER BY next_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_avg_6,
        LEAD(burst_id, 2) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS next_burst_id
    FROM joined
),
flagged AS (
    SELECT *, CASE WHEN prev_paid_at - LAG(prev_paid_at) OVER (PARTITION BY status ORDER BY prev_paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY prev_paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    method,
    amount,
    paid_at,
    status,
    first_known_method,
    amount_dense_rank,
    row_seq,
    prev_paid_at
FROM sessions_payments
