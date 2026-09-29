SELECT dept_name, COUNT(budget) AS budget_count
FROM departments
WHERE dept_name IN ('Marketing', 'Legal', 'Engineering')
GROUP BY dept_name
HAVING COUNT(budget) > 4
ORDER BY 2 DESC, dept_name DESC;
