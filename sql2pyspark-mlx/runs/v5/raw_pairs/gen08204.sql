WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(channel) AS channel_lower,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM orders
    WHERE store_id IS NOT NULL
      AND customer_id IN (13, 16, 17, 20)
),
cleaned_orders AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        amount,
        discount,
        shipping_fee,
        channel_lower,
        CASE WHEN shipping_fee >= 31 THEN 'High' WHEN shipping_fee >= 9 THEN 'Medium' ELSE 'Low' END AS shipping_fee_tier,
        TRIM(status) AS clean_status
    FROM cleaned
    WHERE order_date > DATE '2023-05-07'
      AND store_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        channel,
        discount,
        shipping_fee,
        shipping_fee_tier,
        LAG(shipping_fee, 1) OVER (PARTITION BY channel ORDER BY order_date ASC, id ASC) AS prev_shipping_fee,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY discount DESC, id ASC) AS discount_bucket,
        CUME_DIST() OVER w_running AS shipping_fee_cume_dist
    FROM cleaned_orders
    WINDOW w_running AS (ORDER BY shipping_fee DESC NULLS FIRST)
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, p.name
    FROM metrics a
    INNER JOIN customers p
        ON a.customer_id <=> p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        shipping_fee_tier,
        prev_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel, store_id ORDER BY discount_bucket DESC, id ASC) AS event_seq,
        SUM(shipping_fee) OVER (PARTITION BY store_id ORDER BY prev_shipping_fee ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_shipping_fee,
        FIRST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY channel ORDER BY shipping_fee_cume_dist ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee,
        DENSE_RANK() OVER (ORDER BY discount DESC) AS discount_dense_rank,
        NTILE(4) OVER (ORDER BY shipping_fee_cume_dist DESC, id ASC) AS shipping_fee_cume_dist_bucket
    FROM joined
    WHERE shipping_fee > 17
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.shipped_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
joined_orders AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.shipping_fee_tier,
        s.event_seq,
        s.first_known_shipping_fee,
        s.shipping_fee_cume_dist_bucket,
        b.credit_limit,
        b.city,
        b.signup_date
    FROM latest_match s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
)
-- Grouped metrics
SELECT
    customer_id,
    city,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS event_seq_per_bangkok,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(event_seq) FILTER (WHERE city = 'Tokyo') AS tokyo_total_event_seq,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(DISTINCT city) AS distinct_cities,
    GROUPING_ID() AS grouping_level
FROM joined_orders
GROUP BY GROUPING SETS ((customer_id, city), (customer_id), ())
ORDER BY distinct_cities ASC;
