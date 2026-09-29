WITH department_totals AS (
    SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) level_values, ROUND(AVG(salary), 2) avg_salary
    FROM employees
    GROUP BY dept_id
)
SELECT d.budget, dt.level_values, dt.avg_salary
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id;
