SELECT id, budget, location, dept_name, CONCAT_WS('-', location, dept_name) AS location_dept_name_key FROM departments WHERE dept_name = 'Marketing' OR dept_name IS NULL;
