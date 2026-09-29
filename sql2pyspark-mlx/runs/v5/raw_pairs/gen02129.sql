SELECT city, CASE WHEN city IN ('Berlin', 'Tokyo') THEN 'priority' ELSE 'other' END AS city_group, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY city, CASE WHEN city IN ('Berlin', 'Tokyo') THEN 'priority' ELSE 'other' END
