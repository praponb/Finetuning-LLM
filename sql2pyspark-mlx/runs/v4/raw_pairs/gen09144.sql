select avg(budget) as avg_budget, count(case when location = 'Singapore' then 1 end) as singapore_count
from departments
where budget < 1869000 and location != 'Chiang Mai'
order by avg_budget desc
