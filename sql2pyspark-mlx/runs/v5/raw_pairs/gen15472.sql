select id, dept_name, concat(location, '/', dept_name) as location_dept_name from departments where location = 'New York' and location != 'Bangkok';
