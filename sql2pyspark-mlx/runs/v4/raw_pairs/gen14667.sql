SELECT
    e.id,
    e.level,
    e.hire_date,
    d.budget,
    d.dept_name,
    COALESCE(d.dept_name, 'unassigned') AS dept_name_filled
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;
