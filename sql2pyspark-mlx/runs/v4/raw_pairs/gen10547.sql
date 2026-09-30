SELECT *, CONCAT_WS(',', location, dept_name) location_dept_name_key FROM departments WHERE dept_name IN ('Finance', 'Engineering', 'Marketing') OR budget >= 2164000;
