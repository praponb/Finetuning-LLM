select case when budget >= 4082000 then 'high' when budget >= 1915000 then 'mid' else 'low' end as budget_bucket, sum(budget) as total_budget, sum(sum(budget)) over (order by case when budget >= 4082000 then 'high' when budget >= 1915000 then 'mid' else 'low' end) as cumulative_budget
from departments
where budget is not null
group by case when budget >= 4082000 then 'high' when budget >= 1915000 then 'mid' else 'low' end
order by total_budget desc;
