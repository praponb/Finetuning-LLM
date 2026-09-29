SELECT QUARTER(opened_date) AS opened_quarter, region, SORT_ARRAY(COLLECT_SET(region)) AS region_list
FROM stores
WHERE NOT (opened_date < '2025-08-07' AND region = 'Central')
GROUP BY opened_quarter, region
ORDER BY opened_quarter;
