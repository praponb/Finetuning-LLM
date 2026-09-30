SELECT *
FROM (
    SELECT MONTH(hire_date) AS hire_month, MIN(salary) AS min_salary, ROUND(STDDEV(salary), 2) AS stddev_salary
    FROM employees
    GROUP BY MONTH(hire_date)
) AS s
WHERE stddev_salary >= 62
ORDER BY min_salary DESC, hire_month DESC
