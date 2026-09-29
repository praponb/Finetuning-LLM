select
    d.id,
    d.budget,
    d.dept_name,
    d.location
from departments d
where d.budget between 2677000 and 3715000 and not exists (select 1 from employees e where e.dept_id = d.id)
