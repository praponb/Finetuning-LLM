SELECT
    d.dept_name,
    d.budget,
    e.email,
    e.level
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id
ORDER BY level DESC, dept_name;
