select d.id, d.dept_name, d.budget, d.location from departments d where d.id in (select dept_id from employees);
