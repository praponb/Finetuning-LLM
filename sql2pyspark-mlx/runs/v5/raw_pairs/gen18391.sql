select id, location, budget, lead(budget, 1) over (partition by location order by budget, id) as next_budget from departments
