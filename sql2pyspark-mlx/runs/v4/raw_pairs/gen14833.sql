SELECT CASE WHEN departments.dept_name IN ('Sales', 'Marketing') THEN 'group_a' ELSE 'other' END AS dept_name_group, departments.location, COUNT(DISTINCT employees.salary) AS unique_managers
FROM employees
FULL OUTER JOIN departments ON employees.dept_id = departments.id
GROUP BY CASE WHEN departments.dept_name IN ('Sales', 'Marketing') THEN 'group_a' ELSE 'other' END, departments.location
ORDER BY departments.location ASC, dept_name_group ASC
LIMIT 1;
