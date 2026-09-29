select
    d.budget,
    d.dept_name,
    d.location,
    e.email,
    e.dept_id
from departments d
left join employees e on e.dept_id = d.id;
