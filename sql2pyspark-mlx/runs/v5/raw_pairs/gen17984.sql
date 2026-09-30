WITH cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        LOWER(store_name) store_name_lower
    FROM stores
    WHERE opened_date >= TO_DATE('2023-09-17')
),
valid AS (
    SELECT *, UPPER(TRIM(store_name)) store_name_normalized
    FROM cleaned
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'North'
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.quantity > 171
        )
),
not_flagged AS (
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date BETWEEN '2023-12-26' AND '2025-07-01'
        )
),
enriched AS (
    SELECT
        *,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft,
        LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY sqft, id) next_sqft,
        FIRST_VALUE(region, TRUE) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_region
    FROM not_flagged
),
cleaned_stores AS (
    SELECT
        id,
        store_name,
        city,
        store_name_normalized,
        running_sqft,
        next_sqft,
        first_known_region,
        CASE WHEN region = 'North' THEN 1 ELSE 0 END is_north
    FROM enriched
    WHERE store_name IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY is_north DESC NULLS FIRST) AS dedup_rank
        FROM cleaned_stores
    ) ranked
    WHERE dedup_rank = 1
)
SELECT
    city,
    COUNT_IF(city = 'Tokyo') AS tokyo_events,
    COUNT_IF(city = 'London') AS london_events,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
    SUM(is_north) FILTER (WHERE city = 'Berlin') AS berlin_total_is_north,
    SUM(CASE WHEN city = 'Berlin' THEN running_sqft ELSE 0 END) AS berlin_running_sqft,
    GROUPING_ID() AS grouping_level
FROM ranked
GROUP BY CUBE (city);
