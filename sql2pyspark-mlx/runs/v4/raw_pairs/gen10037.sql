SELECT d.dept_name, e.hire_date
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.id
WHERE e.email LIKE '%@mail.co.th'
ORDER BY dept_name, hire_date
