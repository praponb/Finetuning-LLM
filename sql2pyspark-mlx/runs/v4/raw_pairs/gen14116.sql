WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(order_date, 15) order_date_plus_15d,
        CONCAT(channel, ' - ', status) channel_status,
        COALESCE(channel, 'N/A') channel_filled
    FROM orders
    WHERE customer_id IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.shipped_date <= s.order_date
        )
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        p.store_name,
        p.region
    FROM kept a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(order_date, order_date_plus_15d) days_order_date_plus_15d_to_order_date
    FROM enriched
    WHERE customer_id IS NOT NULL
      AND order_date <= '2023-11-30'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        order_date_plus_15d,
        channel_status,
        channel_filled,
        store_name,
        region,
        days_order_date_plus_15d_to_order_date,
        ROW_NUMBER() OVER (ORDER BY amount, id) event_seq,
        MIN(days_order_date_plus_15d_to_order_date) OVER (PARTITION BY status) status_min_days_order_date_plus_15d_to_order_date,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC) shipping_fee_dense_rank,
        NTILE(10) OVER (PARTITION BY channel, region ORDER BY discount, id) discount_bucket
    FROM filtered
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('WEEK', order_date_plus_15d) AS week_start,
        CASE WHEN amount >= 1590 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        COUNT_IF(channel = 'store') AS store_events,
        COUNT_IF(status = 'SHIPPED') AS shipped_events,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count
    FROM with_windows
    GROUP BY DATE_TRUNC('WEEK', order_date_plus_15d), CASE WHEN amount >= 1590 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    week_start,
    amount_tier,
    channel_list,
    store_events,
    status_list,
    partner_count
FROM stats;
