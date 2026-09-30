select budget_bucket, budget_count, cnt
from (
    select case when budget >= 3640000 then 'high' when budget >= 1091000 then 'mid' else 'low' end as budget_bucket, count(budget) as budget_count, count(*) as cnt
    from departments
    group by case when budget >= 3640000 then 'high' when budget >= 1091000 then 'mid' else 'low' end
) as s
where cnt > 6;
