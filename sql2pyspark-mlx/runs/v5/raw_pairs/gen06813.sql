WITH with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_sale_date
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_revenue,
        top1.store_sale_units
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue, c.units AS store_sale_units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_stores AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_discount
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.discount AS order_discount
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date <= s.opened_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY store_sale_revenue NULLS LAST, id) AS latest_rank
    FROM latest_match_stores
    QUALIFY latest_rank = 1
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.shipping_fee,
        latest.amount
    FROM latest s
    JOIN LATERAL (
            SELECT c.shipping_fee, c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END AS shipping_fee_tier,
    city,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN city = 'Berlin' THEN 1 END), 0), 2) AS amount_per_berlin,
    COUNT_IF(city = 'New York') AS new_york_events,
    APPROX_PERCENTILE(store_sale_revenue, 0.9) AS store_sale_revenue_p90,
    SUM(CASE WHEN city = 'Bangkok' THEN store_sale_units ELSE 0 END) AS bangkok_store_sale_units,
    GROUPING(CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM with_last
GROUP BY CUBE (CASE WHEN shipping_fee >= 36 THEN 'HIGH' ELSE 'LOW' END, city)
