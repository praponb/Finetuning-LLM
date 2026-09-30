select d.dept_name, count(e.id) as employee_count from departments d left join employees e on d.id = e.dept_id group by d.dept_name;
