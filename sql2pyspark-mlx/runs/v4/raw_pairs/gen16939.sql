select departments.dept_name, employees.full_name
from departments
left join employees on employees.dept_id = departments.id
where length(employees.full_name) > 7 and not employees.is_remote;
