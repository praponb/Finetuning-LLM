select id, location, budget, rank() over (partition by location order by budget desc) as rnk from departments where dept_name in ('Engineering', 'Marketing')
