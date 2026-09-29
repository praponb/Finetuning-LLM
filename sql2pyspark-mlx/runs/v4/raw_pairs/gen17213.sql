SELECT location, dept_name FROM departments WHERE budget > 2287000 UNION SELECT location, dept_name FROM departments WHERE location <> 'Singapore';
