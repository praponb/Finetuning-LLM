SELECT AVG(age) AS avg_age, COUNT(*) AS row_count, COUNT(age) AS age_count
FROM customers
ORDER BY avg_age DESC;
