SELECT id, country, age, city, ROUND(age / SUM(age) OVER (PARTITION BY country), 3) AS age_share, COUNT(*) OVER (PARTITION BY city) AS city_count FROM customers;
