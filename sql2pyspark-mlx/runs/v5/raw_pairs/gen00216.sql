SELECT country, SUM(age) total_age, SUM(SUM(age)) OVER (ORDER BY country) AS cumulative_age FROM customers WHERE email IS NULL GROUP BY country ORDER BY country DESC
