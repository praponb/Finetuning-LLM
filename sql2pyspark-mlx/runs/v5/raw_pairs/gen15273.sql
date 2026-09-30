select d.dept_name, e.manager_id
from departments d
left join employees e on e.dept_id = d.id
where d.budget < 2006000
order by d.dept_name asc
