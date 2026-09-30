WITH latest_match AS (
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.discount,
        latest.paid_at,
        latest.payment_amount
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.paid_at, c.amount AS payment_amount
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY store_id ORDER BY paid_at, id) AS prev_paid_at
    FROM latest_match
),
gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
islands AS (
    SELECT
        id,
        store_id,
        paid_at,
        SUM(is_new_session) OVER (PARTITION BY store_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
with_last AS (
    SELECT s.*, x.unit_price
    FROM islands s
    LEFT JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_orders AS (
    SELECT
        s.*,
        latest.payment_method,
        latest.payment_status
    FROM with_last s
    JOIN LATERAL (
            SELECT c.method AS payment_method, c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
wide AS (
    SELECT *
    FROM (
        SELECT
            payment_method,
            payment_status,
            unit_price
        FROM with_last_orders
    ) src
    PIVOT (
        SUM(unit_price)
        FOR payment_status IN ('SUCCESS', 'FAILED')
    )
)
SELECT
    payment_method,
    SUCCESS,
    FAILED
FROM wide
ORDER BY FAILED, SUCCESS DESC;
