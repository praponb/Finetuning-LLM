WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(rating, 0) AS rating_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_filled NULLS LAST, id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        UPPER(supplier_name) AS supplier_name_upper
    FROM deduped
    WHERE city IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date > '2024-11-01'
        )
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
        SIZE(COLLECT_SET(city)) AS num_cities,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        COUNT(DISTINCT country) AS distinct_countries,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT_IF(country = 'UK') AS uk_events
    FROM eligible
    GROUP BY city
)
SELECT
    city,
    berlin_rows,
    country_list,
    distinct_ids
FROM summary
ORDER BY distinct_ids, city DESC;
