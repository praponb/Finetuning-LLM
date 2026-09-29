select d.id, d.dept_name, d.budget, d.location from departments d where d.location in ('New York', 'Chiang Mai', 'London') and d.id in (select dept_id from employees) order by d.id;
