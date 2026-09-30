WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUBSTRING(method, 1, 4) AS method_prefix
    FROM payments
    WHERE status != 'SUCCESS'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(paid_at) AS paid_at_date,
        HOUR(paid_at) AS paid_at_hour,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM cleaned
    WHERE order_id = 15
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at_date DESC NULLS FIRST, id) AS rank_in_group
        FROM sessions_payments
    ) ranked
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.status AS order_status,
        p.channel
    FROM deduped e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(method)) AS method_title
    FROM enriched
    WHERE method IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_tier,
    method,
    CASE WHEN amount >= 1320 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    MAX(prev_paid_at) AS last_seen,
    APPROX_PERCENTILE(session_id, ARRAY(0.25, 0.5, 0.75)) AS session_id_quartiles,
    MAX(session_id) FILTER (WHERE method = 'paypal') AS paypal_max_session_id,
    MIN(amount) AS min_amount,
    ROUND(SUM(is_new_burst) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS is_new_burst_per_failed,
    GROUPING_ID() AS grouping_id
FROM cleaned_payments
GROUP BY GROUPING SETS ((CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, method, CASE WHEN amount >= 1320 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN session_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, method), ())
ORDER BY session_id_tier DESC
