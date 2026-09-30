SELECT CASE WHEN country IN ('UK', 'SG') THEN 'core' ELSE 'other' END AS country_group, SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END) AS berlin_count
FROM suppliers
GROUP BY CASE WHEN country IN ('UK', 'SG') THEN 'core' ELSE 'other' END
HAVING SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END) < 77
ORDER BY country_group DESC;
