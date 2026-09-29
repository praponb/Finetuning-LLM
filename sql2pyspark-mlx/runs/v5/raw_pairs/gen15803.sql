select e.id, e.level
from employees e
where e.dept_id in (select id from departments where location is null)
order by level desc
