select employees.id, employees.is_remote, departments.dept_name
from employees
join departments on employees.dept_id = departments.id
where employees.full_name like 'K%';
