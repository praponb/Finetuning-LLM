WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.quantity = 6
        )
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.shipment_status
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM with_last
    EXCEPT DISTINCT
    SELECT store_id, id
    FROM orders
    WHERE order_date > '2024-09-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    JOIN matched_keys k ON s.store_id = k.store_id AND s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_SUB(order_date, 1) AS order_date_minus_1d,
        COALESCE(shipment_status, 'N/A') AS shipment_status_filled
    FROM verified
    WHERE order_date > TO_DATE('2023-03-30')
      AND shipment_status IS NOT DISTINCT FROM 'LOST'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT channel, customer_id
    FROM base
    EXCEPT DISTINCT
    SELECT channel, customer_id
    FROM orders
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.channel = k.channel AND s.customer_id = k.customer_id
),
matched_keys_2 AS (
    -- Set intersection on keys
    SELECT channel, customer_id
    FROM kept_rows
    INTERSECT ALL
    SELECT channel, customer_id
    FROM orders
    WHERE channel IS NOT NULL
),
verified_orders AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    JOIN matched_keys_2 k ON s.channel = k.channel AND s.customer_id = k.customer_id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity <= 6
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        status,
        CASE WHEN shipping_fee >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END AS shipping_fee_tier,
        SUM(CASE WHEN channel = 'mobile' THEN amount ELSE 0 END) AS mobile_amount,
        COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
        SUM(CASE WHEN shipment_status = 'IN_TRANSIT' THEN shipping_fee ELSE 0 END) AS in_transit_shipping_fee,
        STDDEV_SAMP(discount) AS discount_stddev,
        SIZE(COLLECT_SET(channel)) AS num_channels,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        GROUPING(status) AS grouping_id
    FROM eligible
    GROUP BY CUBE (status, CASE WHEN shipping_fee >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    status,
    mobile_amount,
    in_transit_shipping_fee,
    distinct_customer_ids,
    grouping_id,
    CASE WHEN status RLIKE '^(CANCELLED|PENDING)$' THEN 'GROUP_B' WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS status_zone
FROM totals
ORDER BY mobile_amount NULLS FIRST
