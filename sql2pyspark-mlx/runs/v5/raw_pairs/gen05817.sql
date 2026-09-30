WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        rating,
        ROUND(rating * 0.85, 2) AS rating_adjusted,
        COALESCE(city, 'OTHER') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN country = 'TH' THEN 1 ELSE 0 END AS is_th
    FROM filtered
    WHERE rating_adjusted IS NULL
),
valid_suppliers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        LENGTH(country) AS country_length
    FROM valid
    WHERE rating_adjusted <= 56
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        latest.brand,
        latest.cost
    FROM valid_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.brand, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY brand ORDER BY cost DESC) AS cost_dense_rank,
        SUM(cost) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM latest_match
)
-- Multi-level aggregation
SELECT
    CASE WHEN cost >= 770 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    brand,
    COUNT_IF(brand = 'Umbrella') AS umbrella_events,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list_2,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN cost >= 770 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT_IF(brand = 'Umbrella') DESC) AS umbrella_events_dense_rank
FROM metrics
GROUP BY CASE WHEN cost >= 770 THEN 'PREMIUM' ELSE 'STANDARD' END, brand;
