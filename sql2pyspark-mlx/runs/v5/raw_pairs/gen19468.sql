WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        INITCAP(TRIM(channel)) channel_title,
        discount - shipping_fee discount_minus_shipping_fee,
        COALESCE(channel, 'N/A') channel_filled
    FROM orders
    WHERE customer_id IS NOT NULL
      AND order_date >= DATE '2023-08-04'
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.channel_filled,
        x.payment_status,
        x.payment_method
    FROM valid s
    JOIN LATERAL (
            SELECT c.status AS payment_status, c.method AS payment_method
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Attach stores attributes
    SELECT
        e.*,
        p.sqft,
        p.city
    FROM with_latest e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
enriched_orders AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.channel_filled,
        s.payment_status,
        s.city,
        p2.opened_date,
        p2.city AS store_city
    FROM enriched s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        channel_filled,
        city,
        FIRST_VALUE(store_city, TRUE) OVER (PARTITION BY store_city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_store_city,
        LAST_VALUE(payment_status) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_payment_status,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY payment_status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_city
    FROM enriched_orders
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(city) city_lower
    FROM with_windows
    WHERE store_id IN (25, 34)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.first_known_store_city,
        a.city_lower,
        b.city AS store_city,
        b.region
    FROM base a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
-- Multi-level aggregation
SELECT
    store_city,
    region,
    SIZE(COLLECT_SET(store_city)) AS num_store_cities,
    COUNT_IF(region = 'Central') AS central_events,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    SIZE(COLLECT_SET(store_city)) AS num_store_cities_2,
    COUNT(*) AS row_count
FROM with_dims
GROUP BY GROUPING SETS ((store_city, region), (store_city), ())
