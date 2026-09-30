SELECT segment, country, ROUND(age, 2) age_rounded
FROM customers
WHERE city IN ('Bangkok', 'Berlin', 'Tokyo') AND is_active
ORDER BY segment;
