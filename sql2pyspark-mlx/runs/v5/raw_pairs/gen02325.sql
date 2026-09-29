select
    id,
    location,
    budget,
    min(budget) over (partition by location) as location_min_budget
from departments
where dept_name = 'HR'
order by budget, 4 desc
