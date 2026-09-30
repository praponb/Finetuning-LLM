SELECT *, CASE WHEN city IN ('Berlin', 'Singapore') THEN 'Priority' ELSE 'Other' END AS city_group, ROUND(sqft, 1) AS sqft_rounded
FROM stores
WHERE opened_date > '2024-01-01';
