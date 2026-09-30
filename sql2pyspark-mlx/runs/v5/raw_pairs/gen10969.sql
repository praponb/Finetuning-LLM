WITH src AS (
    SELECT *, CONCAT(region, ', ', store_name) AS region_store_name
    FROM stores
    WHERE YEAR(opened_date) = 2023 AND store_name IS NOT NULL
),
agg AS (
    SELECT region, SUM(sqft) AS total_sqft
    FROM src
    GROUP BY region
)
SELECT *
FROM agg
WHERE total_sqft >= 40000
ORDER BY total_sqft DESC, region DESC
