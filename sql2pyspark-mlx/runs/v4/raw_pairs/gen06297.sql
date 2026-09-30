WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        DATE_FORMAT(paid_at, 'yyyy-MM-dd') AS paid_at_label,
        COALESCE(method, 'DIRECT') AS method_clean
    FROM payments
    WHERE order_id IS NOT NULL
),
base_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        HOUR(paid_at) AS paid_at_hour,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM base
    WHERE paid_at > TIMESTAMP '2023-12-08 04:00:00'
      AND paid_at >= '2025-05-17'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM base_payments
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        paid_at_label,
        method_clean,
        paid_at_hour,
        resolved_method,
        new_group_flag,
        session_no,
        MAX(amount) OVER w_ordered AS amount_max_90d,
        COUNT(paid_at_hour) OVER (PARTITION BY method) AS method_count_paid_at_hour,
        LAST_VALUE(new_group_flag) IGNORE NULLS OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_new_group_flag,
        NTILE(10) OVER (PARTITION BY order_id, method ORDER BY new_group_flag DESC, id) AS new_group_flag_bucket,
        LAST_VALUE(paid_at_hour) IGNORE NULLS OVER w_recent AS last_known_paid_at_hour
    FROM sessions_payments
    WINDOW w_ordered AS (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last_known_paid_at_hour DESC NULLS FIRST, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag_2
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag_2) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.amount AS order_amount,
        b.shipping_fee
    FROM islands a
    INNER JOIN orders b
        ON a.order_id = b.id
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.paid_at_label,
    s.method_clean,
    s.paid_at_hour,
    s.resolved_method,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM with_dims s
ORDER BY paid_at DESC;
