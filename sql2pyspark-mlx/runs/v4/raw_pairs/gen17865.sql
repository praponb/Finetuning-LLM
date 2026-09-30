select d.id, d.budget, d.location from departments d where d.dept_name = 'Sales' and not exists (select 1 from employees e where e.dept_id = d.id and e.is_remote = false);
