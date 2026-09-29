WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount_2,
        AVG(amount) OVER (ORDER BY paid_at ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        COUNT(amount) OVER (PARTITION BY status, method) AS status_count_amount,
        LEAD(amount, 1) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) AS next_amount
    FROM payments
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(running_amount / SUM(running_amount) OVER (PARTITION BY order_id, status), 4) AS running_amount_share,
        PERCENT_RANK() OVER w_recent AS running_amount_percent_rank,
        LAG(running_amount_2, 1) OVER (ORDER BY amount ASC, id ASC) AS prev_running_amount_2
    FROM metrics
    WINDOW w_recent AS (PARTITION BY order_id ORDER BY running_amount DESC)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        running_amount,
        amount_avg_3_prior,
        status_count_amount,
        next_amount,
        running_amount_share,
        running_amount_percent_rank,
        prev_running_amount_2,
        COALESCE(prev_running_amount_2, 0) AS prev_running_amount_2_filled
    FROM with_windows
    WHERE method IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.order_date,
        o.status AS order_status
    FROM base a
    LEFT JOIN orders o
        ON a.order_id <=> o.id
    WHERE o.amount < 720
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.running_amount,
    s.amount_avg_3_prior,
    s.status_count_amount,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'EXTENDED' WHEN status RLIKE '^(FAILED)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS status_zone,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM with_dims s;
