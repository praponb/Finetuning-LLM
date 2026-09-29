select location, count(budget) as budget_count, sum(case when location = 'London' then 1 else 0 end) as london_count
from departments
where (budget >= 1842000 and location in ('Singapore', 'Tokyo', 'Berlin')) or location not in ('Bangkok', 'Tokyo')
group by location
order by london_count desc, location desc
