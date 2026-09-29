with base as (
    select *
    from departments
    where location = 'Singapore'
),
departments_stats as (
    select case when budget >= 3817000 then 'high' when budget >= 1974000 then 'mid' else 'low' end as budget_bucket, count(budget) as budget_count, count(case when dept_name = 'HR' then 1 end) as hr_count
    from base
    group by case when budget >= 3817000 then 'high' when budget >= 1974000 then 'mid' else 'low' end
)
select budget_bucket, budget_count, hr_count
from departments_stats
order by budget_count desc;
