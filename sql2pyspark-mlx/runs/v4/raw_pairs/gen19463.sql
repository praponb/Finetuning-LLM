WITH department_summary AS (
    SELECT dept_id, SORT_ARRAY(COLLECT_SET(level)) AS level_list, SUM(salary) AS total_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.location, ds.level_list, ds.total_salary
FROM departments d
JOIN department_summary ds ON d.id = ds.dept_id
WHERE ds.total_salary >= 365000
ORDER BY total_salary DESC, location DESC
