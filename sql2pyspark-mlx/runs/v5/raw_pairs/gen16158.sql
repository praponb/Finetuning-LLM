WITH recent_products AS (
    SELECT *, ADD_MONTHS(launch_date, 3) AS launch_date_plus_months
    FROM products
    WHERE supplier_id IN (3, 5, 29)
),
summary AS (
    SELECT CASE WHEN cost >= 620 THEN 'high' WHEN cost >= 210 THEN 'mid' ELSE 'low' END AS cost_bucket, MIN(cost) AS min_cost, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM recent_products
    GROUP BY CASE WHEN cost >= 620 THEN 'high' WHEN cost >= 210 THEN 'mid' ELSE 'low' END
)
SELECT cost_bucket, min_cost, category_list
FROM summary
ORDER BY cost_bucket ASC, min_cost DESC
