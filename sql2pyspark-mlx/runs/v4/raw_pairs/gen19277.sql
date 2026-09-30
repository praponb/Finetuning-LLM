select d.location, e.level
from departments d
inner join employees e on e.dept_id = d.id
