WITH department_totals AS (
    SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    WHERE YEAR(hire_date) >= 2023
    GROUP BY dept_id
)
SELECT d.budget, dt.level_values
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id
ORDER BY d.budget DESC
