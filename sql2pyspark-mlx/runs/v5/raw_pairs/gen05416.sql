SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, ROUND(STDDEV(salary), 2) AS stddev_salary, MIN(salary) AS min_salary
FROM employees
WHERE salary BETWEEN 76000 AND 147000
GROUP BY CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY hire_cohort ASC;
