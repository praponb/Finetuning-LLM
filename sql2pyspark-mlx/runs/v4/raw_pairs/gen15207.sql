select e.id, e.hire_date, e.full_name from employees e where e.dept_id in (7, 24, 26) and e.dept_id in (select id from departments where location <> 'Bangkok') order by e.hire_date, full_name
