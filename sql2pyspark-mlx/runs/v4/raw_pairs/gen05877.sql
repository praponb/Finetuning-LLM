select case when budget >= 4413000 then 'high' when budget >= 1446000 then 'mid' else 'low' end as budget_bucket, count(budget) as budget_count, round((100 * sum(case when location = 'New York' then 1 else 0 end)) / count(*), 1) as pct_new_york
from departments
group by budget_bucket
having pct_new_york >= 52
order by budget_bucket
