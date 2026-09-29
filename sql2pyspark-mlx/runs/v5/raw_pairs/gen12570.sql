WITH base AS (
    SELECT *, CASE WHEN brand IN ('Stark', 'Globex') THEN 'Tier 1' ELSE 'Other' END AS brand_group
    FROM products
    WHERE category = 'Home'
),
summary AS (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, MIN(cost) AS min_cost, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM base
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
)
SELECT *
FROM summary
WHERE min_cost > 460;
