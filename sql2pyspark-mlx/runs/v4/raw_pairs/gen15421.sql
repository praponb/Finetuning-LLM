SELECT QUARTER(signup_date) signup_quarter, SUM(age) total_age, SUM(SUM(age)) OVER (ORDER BY QUARTER(signup_date)) AS cumulative_age
FROM customers
GROUP BY QUARTER(signup_date)
ORDER BY 1
LIMIT 50;
