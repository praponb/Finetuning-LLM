SELECT is_active, SUM(age) AS total_age, DENSE_RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank
FROM customers
GROUP BY is_active;
