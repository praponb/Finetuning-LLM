WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, discount * amount AS discount_times_amount
    FROM orders
    WHERE order_date >= TO_DATE('2024-02-05')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(status) AS status_length,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM base
    WHERE customer_id IS NULL
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid
    INTERSECT
    SELECT id
    FROM orders
    WHERE discount IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date >= s.order_date
        )
),
eligible_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.status IN ('RETURNED', 'IN_TRANSIT')
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN discount >= 0.31 THEN 'LARGE' ELSE 'SMALL' END AS discount_tier,
        CASE WHEN amount >= 1360 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
        ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'PENDING' THEN 1 END), 0), 2) AS shipping_fee_per_pending,
        PERCENTILE_APPROX(discount, ARRAY(0.25, 0.5, 0.75)) AS discount_quartiles,
        AVG(amount) FILTER (WHERE status = 'CANCELLED') AS cancelled_avg_amount,
        COUNT_IF(status = 'COMPLETED') AS completed_events,
        COUNT(*) AS row_count,
        SUM(CASE WHEN status = 'COMPLETED' THEN status_length ELSE 0 END) AS completed_status_length,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN discount >= 0.31 THEN 'LARGE' ELSE 'SMALL' END ORDER BY ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'PENDING' THEN 1 END), 0), 2) DESC) AS shipping_fee_per_pending_dense_rank
    FROM eligible_orders
    GROUP BY 1, 2
)
SELECT
    discount_tier,
    amount_tier,
    discount_quartiles,
    cancelled_avg_amount,
    row_count,
    completed_status_length,
    shipping_fee_per_pending_dense_rank
FROM summary
ORDER BY cancelled_avg_amount DESC NULLS LAST;
