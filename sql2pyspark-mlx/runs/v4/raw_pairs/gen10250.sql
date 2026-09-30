WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, TO_DATE(paid_at) AS paid_at_date
    FROM payments
    WHERE paid_at >= DATE '2024-02-13'
      AND paid_at < '2023-12-01'
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.order_date,
        b.status AS order_status
    FROM base a
    JOIN orders b
        ON a.order_id = b.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_id,
            method,
            status,
            amount
        FROM islands
    ) src
    PIVOT (
        SUM(amount)
        FOR status IN ('FAILED', 'REFUNDED')
    )
)
SELECT
    order_id,
    method,
    FAILED,
    REFUNDED,
    CASE WHEN method RLIKE '^(bank_transfer|cash)$' THEN 'GROUP_B' WHEN method RLIKE '^(card|paypal)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS method_zone
FROM wide
ORDER BY FAILED NULLS LAST;
