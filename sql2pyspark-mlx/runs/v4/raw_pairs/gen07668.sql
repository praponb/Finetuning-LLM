select d.id, d.dept_name, d.location from departments d where exists (select 1 from employees e where e.dept_id = d.id and e.dept_id in (9, 16)) order by d.location;
