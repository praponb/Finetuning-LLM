select e.id, e.dept_id, e.full_name
from employees e
where e.dept_id in (select id from departments where dept_name = 'Marketing')
order by e.full_name desc, e.dept_id desc
