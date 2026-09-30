WITH valid_stores AS (
    SELECT *, DATE_ADD(opened_date, 43) AS opened_date_plus_43d
    FROM stores
    WHERE store_name IS NOT NULL
),
region_summary AS (
    SELECT region, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values
    FROM valid_stores
    GROUP BY region
)
SELECT *
FROM region_summary
ORDER BY 2 DESC
