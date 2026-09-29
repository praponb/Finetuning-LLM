SELECT city, MIN(credit_limit) AS min_credit_limit, ROUND(AVG(age), 2) AS avg_age FROM customers GROUP BY city ORDER BY avg_age, city LIMIT 10;
