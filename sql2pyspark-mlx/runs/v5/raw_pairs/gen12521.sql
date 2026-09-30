WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        order_date,
        status,
        shipping_fee,
        ADD_MONTHS(order_date, 1) AS order_date_plus_months
    FROM orders
    WHERE store_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        status,
        shipping_fee,
        order_date_plus_months,
        LEAD(shipping_fee, 2) OVER (PARTITION BY status ORDER BY shipping_fee, id) AS next_shipping_fee,
        AVG(shipping_fee) OVER (PARTITION BY status ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_3,
        DENSE_RANK() OVER (PARTITION BY customer_id, status ORDER BY shipping_fee DESC NULLS FIRST) AS shipping_fee_dense_rank
    FROM valid
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date >= s.order_date_plus_months
        )
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.cost,
        top1.shipped_date
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.cost, c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date_plus_months
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.shipment_shipped_date
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date AS shipment_shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN shipping_fee_avg_3 >= 34 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_avg_3_tier,
        DATE_TRUNC('QUARTER', order_date_plus_months) AS quarter_start,
        status,
        COUNT(*) FILTER (WHERE status = 'SHIPPED') AS shipped_rows,
        COUNT(*) FILTER (WHERE status = 'PENDING') AS pending_rows,
        ROUND(SUM(shipping_fee) / NULLIF(COUNT(CASE WHEN status = 'PENDING' THEN 1 END), 0), 2) AS shipping_fee_per_pending,
        ROUND(AVG(shipping_fee_avg_3), 2) AS avg_shipping_fee_avg_3,
        COUNT(*) FILTER (WHERE status = 'CANCELLED') AS cancelled_rows,
        APPROX_PERCENTILE(shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_quartiles
    FROM latest_match
    GROUP BY shipping_fee_avg_3_tier, quarter_start, status
)
SELECT
    shipping_fee_avg_3_tier,
    quarter_start,
    status,
    shipped_rows,
    pending_rows,
    avg_shipping_fee_avg_3,
    cancelled_rows,
    shipping_fee_quartiles
FROM grouped
ORDER BY shipping_fee_avg_3_tier DESC
