select e.id, e.full_name, e.level from employees e where e.dept_id in (select id from departments where budget > 1616000) order by e.level asc
