select e.id, e.full_name, e.email
from employees e
where e.dept_id in (select id from departments where dept_name not in ('Marketing', 'Legal'))
