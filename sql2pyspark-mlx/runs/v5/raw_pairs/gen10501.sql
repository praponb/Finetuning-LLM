SELECT
    CASE WHEN sqft >= 15121 THEN 'HIGH' ELSE 'LOW' END AS sqft_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SIZE(COLLECT_SET(region)) AS num_regions,
    SUM(CASE WHEN region = 'Central' THEN sqft ELSE 0 END) AS central_sqft
FROM stores
GROUP BY 1
HAVING COUNT(*) < 1000;
