select
    employees.id,
    employees.hire_date,
    employees.job_title,
    departments.dept_name,
    departments.location,
    departments.budget
from employees
inner join departments on departments.id = employees.dept_id
