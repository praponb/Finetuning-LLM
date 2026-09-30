WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        paid_at,
        UPPER(method) AS method_upper,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-10-10 12:00:00'
      AND method != 'cash'
),
base_payments AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(method, 'N/A') AS method_filled
    FROM base
    WHERE paid_at > TO_DATE('2023-08-16')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at DESC, id) AS event_seq,
        FIRST_VALUE(method) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method_2
    FROM base_payments
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_windows
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.paid_at,
        e.method_filled,
        e.first_known_method,
        e.first_known_method_2,
        e.burst_id,
        o.status,
        o.order_date,
        o.amount
    FROM sessionized e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
-- Join a dimension and aggregate
SELECT
    p.channel,
    a.method,
    COUNT(*) AS num_rows,
    AVG(a.amount) AS avg_amount,
    ROUND(AVG(a.burst_id), 2) AS avg_burst_id,
    GROUPING_ID() AS gid
FROM joined a
LEFT JOIN orders p ON a.order_id = p.id
GROUP BY GROUPING SETS ((p.channel, a.method), (p.channel), ());
