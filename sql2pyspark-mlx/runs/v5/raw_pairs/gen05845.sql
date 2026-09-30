SELECT
    s.id,
    s.location,
    s.budget,
    latest.employee_level,
    latest.salary
FROM departments s
LEFT JOIN LATERAL (
        SELECT c.level AS employee_level, c.salary
        FROM employees c
        WHERE c.dept_id = s.id
        ORDER BY c.hire_date DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
