WITH with_prev AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM payments
),
gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_payments AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
joined AS (
    SELECT e.*, d.discount
    FROM sessions_payments e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY island_id DESC, id ASC) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
with_dims AS (
    SELECT
        src.id,
        src.order_id,
        src.method,
        src.status,
        src.island_id,
        src.rank_in_group,
        b.order_date
    FROM latest src
    JOIN orders b
        ON src.order_id = b.id
)
SELECT
    id,
    order_id,
    rank_in_group,
    order_date
FROM with_dims
