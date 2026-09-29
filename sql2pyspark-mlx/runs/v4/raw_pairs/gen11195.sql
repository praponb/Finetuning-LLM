select d.id, d.location, count(e.id) as employee_count
from departments d
inner join employees e on d.id = e.dept_id
where e.dept_id in (13, 18, 19)
group by d.id, d.location;
