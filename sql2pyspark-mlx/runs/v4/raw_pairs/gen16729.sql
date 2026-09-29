SELECT id, dept_name, budget
FROM departments
WHERE dept_name IS NOT NULL AND dept_name = 'Marketing'
