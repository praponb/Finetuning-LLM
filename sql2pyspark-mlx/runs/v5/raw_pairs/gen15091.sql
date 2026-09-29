WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        TRIM(method) AS clean_method,
        DATE_FORMAT(paid_at, 'yyyy-MM-dd') AS paid_at_label
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-04-24 06:00:00')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM valid
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        method,
        paid_at,
        paid_at_label,
        SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.paid_at,
        e.paid_at_label,
        e.session_no,
        p.shipping_fee
    FROM sessionized e
    LEFT JOIN orders p
        ON e.order_id <=> p.id
    WHERE MONTH(p.order_date) IN (4, 5, 6)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_dims
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no_2
    FROM flagged
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    s.session_no,
    s.is_new_burst,
    s.session_no_2,
    (SELECT MIN(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS min_shipment_cost
FROM sessions_payments s;
