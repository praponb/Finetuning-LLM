WITH prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM payments
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
SELECT
    id,
    method,
    paid_at,
    is_new_session,
    CASE WHEN method RLIKE '^(bank_transfer|cash)$' THEN 'CORE' WHEN method RLIKE '^(card|paypal)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS method_zone
FROM sessions_payments
ORDER BY is_new_session DESC NULLS LAST
