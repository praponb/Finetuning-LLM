WITH cleaned AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CONCAT_WS('/', city, country) city_country_key
    FROM suppliers
    WHERE city NOT IN ('London', 'Berlin', 'New York')
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS dedup_rank
    FROM cleaned
    QUALIFY dedup_rank = 1
),
base AS (
    SELECT *, LOWER(supplier_name) supplier_name_lower
    FROM ranked
    WHERE supplier_name IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'clearance')
        )
)
SELECT
    CASE WHEN rating >= 4.6 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(rating) AS rating_stddev,
    SIZE(COLLECT_SET(city)) AS num_cities,
    GROUPING_ID() AS grouping_level
FROM kept
GROUP BY CUBE (CASE WHEN rating >= 4.6 THEN 'LARGE' ELSE 'SMALL' END);
