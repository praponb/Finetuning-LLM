select
    dept_name,
    round(sum(budget) / nullif(count(case when dept_name = 'Marketing' then 1 end), 0), 2) as budget_per_marketing,
    count(*) as event_count
from departments
group by dept_name
having round(sum(budget) / nullif(count(case when dept_name = 'Marketing' then 1 end), 0), 2) >= 0
order by budget_per_marketing nulls last
