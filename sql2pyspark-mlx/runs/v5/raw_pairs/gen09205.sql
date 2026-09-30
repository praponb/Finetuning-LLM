select e.id, e.level, d.location, d.budget from employees e left outer join departments d on e.dept_id = d.id order by budget
