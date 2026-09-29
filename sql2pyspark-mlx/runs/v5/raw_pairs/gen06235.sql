SELECT
    d.location,
    d.budget,
    t.level_values,
    t.pct_l2
FROM departments d
LEFT OUTER JOIN (SELECT dept_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(level))) AS level_values, ROUND((100 * SUM(CASE WHEN level = 'L2' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_l2
    FROM employees
    GROUP BY dept_id) t ON d.id = t.dept_id;
