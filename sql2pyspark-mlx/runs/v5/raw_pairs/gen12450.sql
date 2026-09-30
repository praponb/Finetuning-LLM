select id, budget, dept_name, location from departments where budget >= (select avg(budget) from departments)
