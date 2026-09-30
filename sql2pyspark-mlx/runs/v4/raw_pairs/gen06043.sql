with filtered_departments as (
    select *
    from departments
    where dept_name <> 'Legal'
),
summary as (
    select case when budget >= 4484000 then 'high' when budget >= 1144000 then 'mid' else 'low' end as budget_bucket, sum(budget) as total_budget, round((100 * sum(case when dept_name = 'Marketing' then 1 else 0 end)) / count(*), 1) as pct_marketing
    from filtered_departments
    group by case when budget >= 4484000 then 'high' when budget >= 1144000 then 'mid' else 'low' end
)
select budget_bucket, total_budget, pct_marketing
from summary
order by pct_marketing, budget_bucket
limit 5
