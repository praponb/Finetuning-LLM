SELECT DATE_FORMAT(hire_date, 'yyyy-MM') AS hire_month, SUM(CASE WHEN level = 'L5' THEN 1 ELSE 0 END) AS l5_count, MAX(salary) AS max_salary
FROM employees
WHERE dept_id IN (3, 9, 25, 26) AND salary <= 63000
GROUP BY DATE_FORMAT(hire_date, 'yyyy-MM')
HAVING max_salary >= 126000
ORDER BY max_salary, l5_count DESC;
