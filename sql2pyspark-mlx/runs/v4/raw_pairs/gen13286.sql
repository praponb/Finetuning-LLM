select location, sum(budget) as total_budget, dense_rank() over (order by sum(budget) desc) as budget_rank from departments where location not in ('Chiang Mai', 'London') group by location
