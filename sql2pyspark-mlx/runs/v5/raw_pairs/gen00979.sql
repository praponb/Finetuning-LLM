SELECT is_active, SUM(CASE WHEN country = 'SG' THEN 1 ELSE 0 END) AS sg_count, ROUND(STDDEV(age), 2) AS stddev_age
FROM customers
GROUP BY 1;
