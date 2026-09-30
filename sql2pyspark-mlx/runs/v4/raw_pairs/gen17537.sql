SELECT YEAR(hire_date) AS hire_year, COUNT(CASE WHEN level = 'L2' THEN 1 END) AS l2_count
FROM employees
WHERE email NOT LIKE '%@gmail.com'
GROUP BY 1
