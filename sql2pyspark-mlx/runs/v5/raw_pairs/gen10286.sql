SELECT departments.id, departments.location, departments.dept_name FROM departments WHERE departments.id IN (SELECT dept_id FROM employees WHERE LOWER(TRIM(full_name)) = 'olivia garcia')
