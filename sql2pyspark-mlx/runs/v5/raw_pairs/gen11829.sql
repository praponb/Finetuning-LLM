SELECT budget, dept_name, CONCAT(location, '/', dept_name) AS location_dept_name, CASE WHEN location = 'New York' THEN 1 ELSE 0 END AS is_new_york FROM departments;
