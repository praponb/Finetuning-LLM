WITH valid AS (
    SELECT
        *,
        ROUND(revenue, 2) revenue_rounded,
        ROUND(units, 2) units_rounded
    FROM store_sales
    WHERE store_id = 5
),
joined AS (
    SELECT
        e.id,
        e.store_id,
        e.sale_date,
        e.units_rounded,
        s.opened_date,
        s.city
    FROM valid e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
with_dims AS (
    SELECT
        s.*,
        b.sqft,
        b.store_name
    FROM joined s
    LEFT JOIN stores b
        ON s.store_id = b.id
),
enriched AS (
    SELECT
        s.id,
        s.store_id,
        s.city,
        s.sqft,
        s.store_name,
        p.store_name AS store_store_name
    FROM with_dims s
    LEFT JOIN stores p
        ON s.store_id = p.id
    WHERE p.city = 'Singapore'
),
latest AS (
    SELECT id, city
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft NULLS FIRST, id) = 1
),
valid_store_sales AS (
    SELECT *, TRIM(city) clean_city
    FROM latest
    WHERE city IS NOT NULL
      AND city <> 'Bangkok'
)
SELECT
    city,
    COUNT_IF(city = 'London') AS london_events,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    COUNT(*) AS event_count,
    COUNT(*) AS event_count_2,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
FROM valid_store_sales
GROUP BY city
