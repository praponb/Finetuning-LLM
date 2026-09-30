select e.id, e.email, d.budget from employees e right join departments d on e.dept_id = d.id where e.dept_id in (26, 33, 39) order by id desc, email
