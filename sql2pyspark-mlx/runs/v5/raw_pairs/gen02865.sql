select e.id, e.email
from employees e
where e.dept_id not in (select id from departments where location in ('New York', 'Singapore'))
order by e.email;
