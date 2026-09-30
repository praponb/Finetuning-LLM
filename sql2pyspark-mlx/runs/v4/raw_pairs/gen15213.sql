select d.location, d.dept_name, d.budget, e.level, e.hire_date from departments d left join employees e on e.dept_id = d.id order by level;
