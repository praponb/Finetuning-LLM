select d.location, e.full_name, e.is_remote
from departments d
inner join employees e on d.id = e.dept_id
order by 1, e.full_name
