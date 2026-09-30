select d.id, d.budget, d.location
from departments d
where not exists (select 1 from employees e where e.dept_id = d.id and e.manager_id is not null)
order by location, id
limit 50;
