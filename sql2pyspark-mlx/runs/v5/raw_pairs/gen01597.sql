WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(paid_at) AS paid_at_date,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE paid_at > DATE '2023-09-14'
      AND order_id = 26
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM base
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.method,
        a.amount,
        a.paid_at,
        a.status,
        a.method_filled,
        a.is_new_burst,
        a.session_no,
        p.status AS order_status,
        p.discount
    FROM islands a
    LEFT JOIN orders p
        ON a.order_id <=> p.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM enriched
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        o.amount AS order_amount,
        o.shipping_fee,
        o.channel
    FROM sessions_payments src
    LEFT JOIN orders o
        ON src.order_id = o.id
    WHERE o.discount IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        method_filled,
        session_no,
        order_status,
        is_new_burst_2,
        LEAD(discount) OVER w_grp AS next_discount,
        LAG(discount) OVER w_grp AS prev_discount,
        LEAD(session_no) OVER w_grp AS next_session_no,
        LAG(session_no) OVER w_grp AS prev_session_no,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM joined
    WINDOW w_grp AS (PARTITION BY channel ORDER BY order_amount, id)
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            order_status,
            session_no
        FROM with_windows
    ) src
    PIVOT (
        MAX(session_no) AS value_sum,
        COUNT(1) AS cnt
        FOR order_status IN ('COMPLETED' AS completed, 'PENDING' AS pending)
    )
)
SELECT
    status,
    completed_value_sum,
    completed_cnt,
    CASE WHEN status RLIKE '^(REFUNDED|FAILED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(SUCCESS)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone
FROM by_type
