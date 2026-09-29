WITH Base AS (
    SELECT *
    FROM products
    WHERE launch_date > '2025-06-01' AND cost < 570
),
agg AS (
    SELECT brand, SORT_ARRAY(COLLECT_SET(category)) AS category_list, MIN(launch_date) AS min_launch_date
    FROM Base
    GROUP BY brand
)
SELECT *
FROM agg
ORDER BY min_launch_date;
