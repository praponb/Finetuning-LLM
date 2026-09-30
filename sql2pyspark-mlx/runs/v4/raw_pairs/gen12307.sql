SELECT country, SUM(age) AS total_age, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank FROM customers GROUP BY country ORDER BY age_rank DESC;
