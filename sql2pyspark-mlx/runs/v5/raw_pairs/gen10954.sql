WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(city, ', ', supplier_name) AS city_supplier_name
    FROM suppliers
    WHERE city IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY rating NULLS LAST) AS rating_percent_rank,
        MAX(rating) OVER w_recent AS city_max_rating,
        LAST_VALUE(rating) IGNORE NULLS OVER w_grp AS last_known_rating
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY city, country), w_grp AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM enriched
    INTERSECT ALL
    SELECT id, city
    FROM suppliers
    WHERE city = 'Berlin'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id AND s.city = k.city
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND LENGTH(c.product_name) >= 9
        )
)
-- Multi-level aggregation
SELECT
    city,
    SUM(last_known_rating) FILTER (WHERE city = 'Singapore') AS singapore_total_last_known_rating,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(country = 'JP') AS jp_events,
    COUNT_IF(city = 'London') AS london_events,
    PERCENTILE_APPROX(rating_share, ARRAY(0.25, 0.5, 0.75)) AS rating_share_quartiles
FROM not_flagged
GROUP BY city;
