SELECT YEAR(signup_date) AS signup_year, SUM(age) AS total_age, SUM(SUM(age)) OVER (ORDER BY YEAR(signup_date)) AS cumulative_age FROM customers WHERE age <> 52 GROUP BY YEAR(signup_date);
