SELECT
    CASE WHEN sqft >= 15593 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    region,
    SUM(CASE WHEN region = 'North' THEN sqft ELSE 0 END) AS north_sqft,
    COUNT(*) AS cnt
FROM stores
GROUP BY CASE WHEN sqft >= 15593 THEN 'LARGE' ELSE 'SMALL' END, region
ORDER BY region;
