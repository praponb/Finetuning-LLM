SELECT id, budget, UPPER(location) AS location_upper FROM departments WHERE dept_name IN ('Marketing', 'Sales', 'HR');
