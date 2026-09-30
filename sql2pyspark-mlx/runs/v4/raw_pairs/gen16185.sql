WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(city) AS city_length,
        SUBSTRING(city, 1, 4) AS city_prefix,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.product_cost
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest_match_suppliers AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.launch_date
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date < s.launch_date
        )
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating,
        COUNT_IF(city = 'New York') AS new_york_events,
        COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
        ROUND(AVG(product_cost), 2) AS avg_product_cost
    FROM eligible
    GROUP BY CASE WHEN rating >= 3.6 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    rating_tier,
    country_list,
    chiang_mai_rating,
    avg_product_cost
FROM daily;
