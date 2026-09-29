select
    d.budget,
    d.dept_name,
    e.full_name,
    e.hire_date
from departments d
left join employees e on e.dept_id = d.id
where d.dept_name in ('Engineering', 'Sales', 'Finance')
