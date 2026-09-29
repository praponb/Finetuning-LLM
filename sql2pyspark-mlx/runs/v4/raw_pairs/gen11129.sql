WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM payments
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY amount DESC) = 1
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM latest
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
sessionized_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        prev_paid_at,
        island_id,
        is_new_session,
        session_no,
        DATEDIFF(CURRENT_DATE(), prev_paid_at) AS days_since_prev_paid_at,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM sessionized_payments
    WHERE TO_DATE(paid_at) < '2024-11-01'
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    method,
    amount,
    paid_at,
    status,
    prev_paid_at,
    island_id,
    is_new_session,
    session_no,
    days_since_prev_paid_at,
    method_clean,
    AVG(days_since_prev_paid_at) OVER (PARTITION BY order_id) AS order_id_avg_days_since_prev_paid_at,
    AVG(days_since_prev_paid_at) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS days_since_prev_paid_at_avg_5,
    SUM(amount) OVER (PARTITION BY status, method ORDER BY CAST(prev_paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d,
    MAX(amount) OVER (ORDER BY CAST(prev_paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_max_90d
FROM valid;
