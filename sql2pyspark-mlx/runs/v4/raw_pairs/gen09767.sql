select d.dept_name, e.dept_id, e.hire_date, e.manager_id from departments d left join employees e on d.id = e.dept_id order by e.hire_date
