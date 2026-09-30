select d.id, d.location from departments d where not exists (select 1 from employees e where e.dept_id = d.id and e.dept_id = 24) order by d.location asc;
