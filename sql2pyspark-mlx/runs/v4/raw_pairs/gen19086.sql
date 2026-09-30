select
    id,
    manager_id,
    dept_id,
    hire_date
from employees
where dept_id in (2, 9, 33)
