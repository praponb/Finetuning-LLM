SELECT s.* FROM departments s WHERE NOT EXISTS (SELECT 1 FROM employees c WHERE c.dept_id = s.id AND LOWER(TRIM(c.full_name)) = 'emma brown')
