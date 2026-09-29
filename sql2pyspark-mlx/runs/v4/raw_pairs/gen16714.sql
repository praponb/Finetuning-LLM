SELECT
    CASE WHEN budget >= 4410000 THEN 'high' WHEN budget >= 940000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    location,
    CASE WHEN location IN ('Tokyo', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END AS location_group,
    COUNT(CASE WHEN location = 'Berlin' THEN 1 END) AS berlin_count
FROM departments
GROUP BY 1, 2, 3
