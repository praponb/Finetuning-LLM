select id, budget, dept_name, location from departments where budget >= (select avg(budget) from departments where budget >= 2992000) order by id desc
