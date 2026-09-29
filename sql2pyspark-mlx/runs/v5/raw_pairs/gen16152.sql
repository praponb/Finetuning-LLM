select d.id, d.dept_name from departments d where exists (select 1 from employees e where e.dept_id = d.id and e.full_name like 'bo%') order by d.id asc, dept_name desc limit 3;
