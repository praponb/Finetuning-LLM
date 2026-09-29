SELECT
    location,
    dept_name,
    budget,
    CASE WHEN location IN ('Bangkok', 'Chiang Mai') THEN 'Tier 1' ELSE 'Other' END AS location_group
FROM departments
WHERE location = 'Tokyo' AND budget >= 2629000
