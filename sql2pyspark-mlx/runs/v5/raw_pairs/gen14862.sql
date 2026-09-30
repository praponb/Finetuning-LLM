select
    d.location,
    d.budget,
    d.dept_name,
    e.email,
    e.salary,
    e.dept_id
from departments d
left join employees e on e.dept_id = d.id;
