WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        opened_date,
        LOWER(region) AS region_lower,
        LENGTH(city) AS city_length,
        COALESCE(region, 'N/A') AS region_filled
    FROM stores
    WHERE city IN ('London', 'Singapore', 'Bangkok')
      AND region IS NOT DISTINCT FROM 'West'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.status = 'SHIPPED'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date >= s.opened_date
        )
),
eligible_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date >= s.opened_date
        )
),
kept_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        region,
        opened_date,
        region_lower,
        city_length,
        CASE WHEN city_length >= 89 THEN 'High' WHEN city_length >= 35 THEN 'Medium' ELSE 'Low' END AS city_length_tier
    FROM kept_stores
    WHERE city_length IS NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(city_length) OVER (PARTITION BY region ORDER BY city_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_city_length,
        MIN(city_length) OVER (PARTITION BY region ORDER BY city_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_city_length,
        MAX(city_length) OVER (PARTITION BY region ORDER BY city_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_city_length,
        SUM(city_length) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_city_length
    FROM cleaned
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    region_lower,
    running_avg_city_length,
    running_min_city_length,
    running_max_city_length,
    running_city_length
FROM scored
ORDER BY running_avg_city_length
