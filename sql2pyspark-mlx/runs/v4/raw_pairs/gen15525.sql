SELECT
    id,
    location,
    budget,
    CASE WHEN location IN ('London', 'Chiang Mai') THEN 'Priority' ELSE 'Other' END AS location_group
FROM departments
ORDER BY location_group DESC;
