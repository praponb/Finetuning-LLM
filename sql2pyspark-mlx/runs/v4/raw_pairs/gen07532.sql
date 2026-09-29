SELECT
    CASE WHEN sqft >= 17328 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    city,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) AS north_sqft,
    MAX(sqft) FILTER (WHERE region = 'West') AS west_max_sqft,
    PERCENTILE_APPROX(sqft, 0.95) AS sqft_p95,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'East' THEN 1 END), 0), 2) AS sqft_per_east,
    PERCENTILE_APPROX(sqft, 0.9) AS sqft_p90
FROM stores
GROUP BY GROUPING SETS ((CASE WHEN sqft >= 17328 THEN 'LARGE' ELSE 'SMALL' END, city, DATE_TRUNC('WEEK', opened_date)), (CASE WHEN sqft >= 17328 THEN 'LARGE' ELSE 'SMALL' END, city), (CASE WHEN sqft >= 17328 THEN 'LARGE' ELSE 'SMALL' END))
HAVING PERCENTILE_APPROX(sqft, 0.95) > 0
