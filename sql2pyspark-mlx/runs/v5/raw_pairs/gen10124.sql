WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        SUBSTRING(status, 1, 2) AS status_prefix,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM orders
    WHERE channel IS DISTINCT FROM 'partner'
      AND order_date > DATE '2023-03-25'
),
enriched AS (
    -- Attach customers attributes
    SELECT e.*, b.name
    FROM base e
    JOIN customers b
        ON e.customer_id = b.id
    WHERE b.email LIKE '%@gmail.com'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND TO_DATE(ch.paid_at) < '2023-12-05'
        )
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, d.region
    FROM eligible s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
enriched_orders AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.sqft,
        b.store_name
    FROM joined src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_quantity
    FROM enriched_orders s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        status,
        shipping_fee,
        status_prefix,
        channel_filled,
        name,
        region,
        sqft,
        NTILE(4) OVER (PARTITION BY region, store_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        AVG(sqft) OVER (ORDER BY discount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sqft_avg_6,
        SUM(sqft) OVER (ORDER BY order_item_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM with_last
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
        AVG(cumulative_sqft) AS avg_cumulative_sqft,
        ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
        SIZE(COLLECT_SET(region)) AS num_regions,
        GROUPING_ID() AS grouping_id
    FROM with_windows
    GROUP BY ROLLUP (CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    shipping_fee_tier,
    avg_cumulative_sqft,
    avg_shipping_fee,
    num_regions
FROM totals
ORDER BY avg_cumulative_sqft DESC;
