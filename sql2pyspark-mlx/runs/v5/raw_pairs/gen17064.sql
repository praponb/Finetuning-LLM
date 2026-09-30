select
    employees.id,
    employees.hire_date,
    employees.email,
    employees.full_name,
    departments.budget
from employees
join departments on employees.dept_id = departments.id
order by id desc
limit 50;
