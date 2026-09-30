SELECT YEAR(signup_date) AS signup_year, city, MAX(age) AS max_age, MIN(credit_limit) AS min_credit_limit, SUM(credit_limit) AS total_credit_limit FROM customers WHERE country = 'DE' GROUP BY 1, 2;
