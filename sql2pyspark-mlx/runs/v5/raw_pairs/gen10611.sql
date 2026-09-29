SELECT dept_name, COUNT(*) AS department_count
FROM departments
GROUP BY dept_name;
