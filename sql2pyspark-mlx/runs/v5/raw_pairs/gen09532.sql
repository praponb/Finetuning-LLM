select departments.dept_name, employees.salary from departments left join employees on employees.dept_id = departments.id order by employees.salary asc
