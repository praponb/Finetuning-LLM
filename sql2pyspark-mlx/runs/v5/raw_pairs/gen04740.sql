select
    employees.job_title,
    employees.hire_date,
    employees.email,
    departments.dept_name,
    departments.budget
from employees
join departments on employees.dept_id = departments.id
order by hire_date desc
