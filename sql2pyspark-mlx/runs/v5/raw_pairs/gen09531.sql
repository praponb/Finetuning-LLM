select d.id, d.dept_name, count(e.id) employee_count from departments d left join employees e on e.dept_id = d.id group by d.id, d.dept_name order by d.dept_name
