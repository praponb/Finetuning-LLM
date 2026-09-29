WITH department_totals AS (
    SELECT dept_id, SUM(CASE WHEN level = 'L2' THEN salary ELSE 0 END) AS l2_salary, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name, dt.l2_salary, dt.level_values
FROM departments d
LEFT JOIN department_totals dt ON d.id = dt.dept_id;
