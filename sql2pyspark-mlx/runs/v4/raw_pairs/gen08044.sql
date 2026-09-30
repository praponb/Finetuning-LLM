select
    location,
    round(avg(budget), 2) as avg_budget,
    sum(case when dept_name = 'HR' then budget else 0 end) as hr_budget,
    sum(budget) as total_budget
from departments
group by location;
