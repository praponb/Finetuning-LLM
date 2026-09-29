SELECT id, location FROM departments WHERE dept_name IN ('Engineering', 'Legal', 'Finance') UNION ALL SELECT id, location FROM departments WHERE dept_name IS NOT NULL;
