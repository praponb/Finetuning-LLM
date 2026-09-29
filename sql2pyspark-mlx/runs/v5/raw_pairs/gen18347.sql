WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at,
        LENGTH(status) AS status_length,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE paid_at > DATE '2023-09-28'
      AND amount <= 1200
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        days_since_paid_at,
        status_length,
        resolved_method,
        CUME_DIST() OVER (PARTITION BY status ORDER BY status_length DESC NULLS LAST) AS status_length_cume_dist,
        NTILE(3) OVER (PARTITION BY status, method ORDER BY status_length DESC, id ASC) AS status_length_bucket,
        AVG(amount) OVER (PARTITION BY order_id, method ORDER BY paid_at ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS amount_avg_2_prior
    FROM base
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
gaps_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM islands
),
islands_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps_2
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.discount,
        b.order_date
    FROM islands_payments src
    LEFT JOIN orders b
        ON src.order_id = b.id
    WHERE b.channel != 'partner'
),
base_payments AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('week', paid_at) AS paid_at_period
    FROM joined
    WHERE order_id IS NOT NULL
      AND paid_at > TIMESTAMP '2023-09-16 07:00:00'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY status_length_bucket ASC, id ASC) AS row_num
        FROM base_payments
    ) ranked
    WHERE row_num = 1
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    s.status,
    s.resolved_method,
    s.status_length_cume_dist,
    s.status_length_bucket,
    s.is_new_session,
    s.session_no,
    CASE WHEN status RLIKE '^(SUCCESS|FAILED)$' THEN 'GROUP_B' WHEN status RLIKE '^(REFUNDED)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone,
    (SELECT MAX(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS max_shipment_cost
FROM latest s
ORDER BY is_new_session DESC NULLS FIRST
