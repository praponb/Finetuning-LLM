WITH recent_stores AS (
    SELECT *, LOWER(region) AS region_lower
    FROM stores
    WHERE region <> 'Central' AND city IN ('London', 'Bangkok')
),
opened_month_summary AS (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, SUM(CASE WHEN region = 'Central' THEN 1 ELSE 0 END) AS central_count
    FROM recent_stores
    GROUP BY DATE_TRUNC('MONTH', opened_date)
)
SELECT opened_month, central_count
FROM opened_month_summary
WHERE central_count > 37
ORDER BY 2, opened_month
LIMIT 3;
