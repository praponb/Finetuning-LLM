SELECT
    e.is_remote AS employee_is_remote,
    m.level AS manager_level,
    m.is_remote AS manager_is_remote,
    m.dept_id AS manager_dept_id
FROM employees e
LEFT OUTER JOIN employees m ON e.manager_id = m.id
WHERE m.manager_id IS NULL
ORDER BY m.level, 4 DESC
