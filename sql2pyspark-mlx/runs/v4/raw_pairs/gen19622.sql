SELECT YEAR(signup_date) AS signup_year, MIN(age) AS min_age FROM customers GROUP BY 1 ORDER BY min_age;
