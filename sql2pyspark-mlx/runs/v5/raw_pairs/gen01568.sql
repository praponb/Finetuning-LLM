SELECT
    city,
    supplier_name,
    country,
    COALESCE(city, 'unassigned') AS city_filled,
    CASE WHEN city IN ('Tokyo', 'Singapore') THEN 'Tier 1' ELSE 'Other' END AS city_group
FROM suppliers
ORDER BY 3 DESC;
