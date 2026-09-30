WITH base AS (
    SELECT *
    FROM suppliers
    WHERE LENGTH(supplier_name) > 12
),
summary AS (
    SELECT
        CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END rating_bucket,
        country,
        SORT_ARRAY(COLLECT_SET(city)) city_list,
        SUM(CASE WHEN country = 'DE' THEN rating ELSE 0 END) de_rating
    FROM base
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END, country
)
SELECT
    rating_bucket,
    country,
    city_list,
    de_rating
FROM summary
WHERE de_rating > 75;
