SELECT dept_name, CONCAT(location, ' - ', dept_name) AS location_dept_name FROM departments WHERE budget > 3791000 ORDER BY location_dept_name DESC
