SELECT CASE WHEN city IN ('New York', 'Singapore') THEN 'group_a' ELSE 'other' END AS city_group, COUNT(*) AS cnt, ROUND(STDDEV(rating), 2) AS stddev_rating
FROM suppliers
WHERE country IN ('US', 'SG', 'DE') AND (rating <= 2.6 OR rating <= 2.3)
GROUP BY city_group
HAVING ROUND(STDDEV(rating), 2) < 34
ORDER BY city_group;
