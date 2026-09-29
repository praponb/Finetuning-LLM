WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(channel, ' - ', status) AS channel_status,
        ROUND(amount * 100, 2) AS adjusted_amount
    FROM orders
    WHERE channel IS NOT DISTINCT FROM 'store'
      AND customer_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        store_id,
        order_date,
        discount,
        shipping_fee,
        adjusted_amount
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date >= s.order_date
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount) OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_avg_3,
        LAG(discount, 2) OVER (PARTITION BY store_id ORDER BY order_date, id) AS prev_discount
    FROM kept
),
enriched AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.discount,
        src.shipping_fee,
        src.discount_avg_3,
        b.region,
        b.sqft
    FROM scored src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
kept_orders AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.order_id IN (7, 18, 19)
        )
)
-- Grouped metrics
SELECT
    store_id,
    CASE WHEN sqft >= 13128 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    MIN(sqft) AS min_sqft,
    PERCENTILE_APPROX(discount_avg_3, 0.5) AS discount_avg_3_p50
FROM kept_orders
GROUP BY 1, 2
ORDER BY sqft_tier NULLS LAST;
