select departments.dept_name, sort_array(collect_set(employees.level)) as level_list, round(avg(employees.salary), 2) as avg_salary
from employees
right join departments on employees.dept_id = departments.id
where departments.budget is not null
group by departments.dept_name
