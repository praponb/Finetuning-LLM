select e.id, e.level, e.salary from employees e where e.dept_id in (select id from departments where dept_name = 'Sales') order by level, id limit 1;
