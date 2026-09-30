WITH Base AS (
    SELECT *, TRIM(store_name) AS clean_store_name
    FROM stores
    WHERE region IN ('West', 'North', 'South') AND city != 'Bangkok'
),
opened_month_summary AS (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(CASE WHEN city = 'Singapore' THEN sqft ELSE 0 END) AS singapore_sqft, SUM(CASE WHEN city = 'Tokyo' THEN sqft ELSE 0 END) AS tokyo_sqft
    FROM Base
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
)
SELECT opened_month, singapore_sqft, tokyo_sqft
FROM opened_month_summary
WHERE tokyo_sqft < 45
ORDER BY tokyo_sqft DESC;
