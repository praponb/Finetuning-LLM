SELECT dept_name, CONCAT(dept_name, ' ', location) AS dept_name_location, COALESCE(location, 'N/A') AS location_filled FROM departments WHERE dept_name = 'Sales' AND budget IS NOT NULL;
