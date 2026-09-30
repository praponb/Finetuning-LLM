select e.id, e.full_name, d.location
from employees e
left join departments d on e.dept_id = d.id
