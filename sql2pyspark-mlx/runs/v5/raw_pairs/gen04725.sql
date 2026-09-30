select id, dept_name, budget, rank() over (partition by dept_name order by budget) as rnk from departments
