SELECT s.*, x.employee_level
FROM departments s
LEFT JOIN LATERAL (
        SELECT c.level AS employee_level
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.hire_date DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
