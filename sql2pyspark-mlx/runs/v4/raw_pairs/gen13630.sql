SELECT departments.budget, employees.level, employees.hire_date
FROM departments
INNER JOIN employees ON departments.id = employees.dept_id
WHERE employees.manager_id IN (4, 17, 32, 33) AND departments.budget > 1832000
