select d.id, d.dept_name, d.location from departments d where exists (select 1 from employees e where e.dept_id = d.id and e.salary > 103000);
