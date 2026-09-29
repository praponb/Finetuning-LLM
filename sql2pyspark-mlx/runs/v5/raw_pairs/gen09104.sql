select d.location, d.dept_name, d.budget, e.dept_id from departments d left join employees e on e.dept_id = d.id where e.email like '%@outlook.com' order by budget desc;
