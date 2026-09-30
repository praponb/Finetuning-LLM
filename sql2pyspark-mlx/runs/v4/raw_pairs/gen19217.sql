select
    e.id,
    e.dept_id,
    e.hire_date,
    d.dept_name,
    d.location
from employees e
left join departments d on e.dept_id = d.id
where e.hire_date between '2025-02-27' and '2025-07-01'
