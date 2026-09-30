WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, discount * shipping_fee AS discount_times_shipping_fee
    FROM orders
    WHERE customer_id IS NOT NULL
      AND store_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount) OVER w_part AS status_sum_discount,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS FIRST) AS amount_dense_rank,
        MIN(discount) OVER (PARTITION BY customer_id) AS customer_id_min_discount,
        LEAD(amount, 1) OVER (ORDER BY amount, id) AS next_amount,
        LEAD(shipping_fee, 1) OVER (PARTITION BY status, store_id ORDER BY discount_times_shipping_fee, id) AS next_shipping_fee
    FROM cleaned
    WINDOW w_part AS (PARTITION BY status)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.carrier = 'Kerry'
        )
),
not_flagged_orders AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.quantity BETWEEN 4 AND 7
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date >= s.order_date
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', order_date) AS week_start,
        AVG(discount_times_shipping_fee) FILTER (WHERE status = 'RETURNED') AS returned_avg_discount_times_shipping_fee,
        ROUND(SUM(customer_id_min_discount) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS customer_id_min_discount_per_web,
        SIZE(COLLECT_SET(status)) AS num_status
    FROM eligible
    GROUP BY DATE_TRUNC('WEEK', order_date)
    HAVING returned_avg_discount_times_shipping_fee >= 0
)
SELECT week_start, num_status
FROM summary
ORDER BY week_start DESC, num_status;
