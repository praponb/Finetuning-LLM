SELECT id, country, age, MIN(age) OVER (PARTITION BY country) AS country_min_age FROM customers WHERE signup_date < '2023-06-01';
