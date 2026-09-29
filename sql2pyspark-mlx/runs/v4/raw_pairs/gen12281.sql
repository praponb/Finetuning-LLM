select e.id, e.level, e.hire_date, e.salary, d.budget, d.location, d.dept_name from employees e left join departments d on e.dept_id = d.id order by level desc
