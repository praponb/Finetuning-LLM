WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date,
        INITCAP(TRIM(region)) AS region_title
    FROM stores
    WHERE store_name IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date < s.opened_date
        )
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.opened_date,
        s.sqft,
        s.days_since_opened_date,
        s.region_title,
        top1.order_order_date
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_sqft,
        COUNT(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_sqft,
        AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_sqft
    FROM with_last
    WHERE order_order_date > '2024-07-27'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(last4_avg_sqft, 2) AS last4_avg_sqft_rounded
    FROM scored
    WHERE opened_date >= DATE '2023-05-05'
      AND opened_date <= '2024-01-01'
),
with_last_stores AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.last4_avg_sqft,
        x.quantity
    FROM filtered s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_name,
    last4_avg_sqft,
    CASE WHEN store_name RLIKE '^(Old Town|Downtown Express)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS store_name_zone
FROM with_last_stores
