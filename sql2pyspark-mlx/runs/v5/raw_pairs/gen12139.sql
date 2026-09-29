select *, concat_ws('-', location, dept_name) location_dept_name_key from departments where budget is not null
