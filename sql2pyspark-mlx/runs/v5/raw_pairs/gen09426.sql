SELECT QUARTER(hire_date) AS hire_quarter, AVG(salary) AS avg_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
FROM employees
GROUP BY hire_quarter
ORDER BY hire_quarter DESC
