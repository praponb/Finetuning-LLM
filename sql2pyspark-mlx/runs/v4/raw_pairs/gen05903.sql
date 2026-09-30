SELECT CASE WHEN city IN ('Tokyo', 'Bangkok') THEN 'tier_1' ELSE 'other' END AS city_group, AVG(sqft) AS avg_sqft
FROM stores
WHERE region IN ('North', 'South') AND sqft = 11486 AND store_name LIKE 'Air%'
GROUP BY 1
HAVING avg_sqft > 4800;
