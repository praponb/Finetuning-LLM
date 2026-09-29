select departments.dept_name, count(employees.id) as employee_count
from departments
left join employees on employees.dept_id = departments.id
where employees.full_name like 'FR%'
group by departments.dept_name
order by departments.dept_name
