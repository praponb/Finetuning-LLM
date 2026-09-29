select
    employees.id,
    employees.level,
    employees.job_title,
    employees.hire_date
from employees
where employees.level in ('L5', 'L4', 'L1') and employees.dept_id not in (select id from departments where dept_name = 'Sales')
