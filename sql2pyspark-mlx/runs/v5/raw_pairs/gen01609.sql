SELECT QUARTER(hire_date) AS hire_quarter, ROUND(STDDEV(salary), 2) AS stddev_salary, AVG(salary) AS avg_salary
FROM employees
GROUP BY QUARTER(hire_date)
ORDER BY avg_salary;
