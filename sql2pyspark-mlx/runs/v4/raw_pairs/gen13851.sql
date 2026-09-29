SELECT country, MAX(age) - MIN(age) AS age_range, SUM(age) AS total_age FROM customers GROUP BY country;
