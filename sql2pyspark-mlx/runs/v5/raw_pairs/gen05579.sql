SELECT
    d.dept_name,
    e.hire_date,
    e.level,
    CASE WHEN d.dept_name = 'Finance' THEN 1 ELSE 0 END AS is_finance
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE e.level IN ('L4', 'L1') AND d.budget <= 2909000
ORDER BY hire_date DESC;
