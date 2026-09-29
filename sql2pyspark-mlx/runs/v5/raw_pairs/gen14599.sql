with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(budget * 0.9, 2) as budget_adjusted,
        coalesce(location, 'N/A') as resolved_location
    from departments
    where location in ('Bangkok', 'Berlin')
)
-- Aggregate with conditional metrics
select
    case when budget >= 4299000 then 'LARGE' else 'SMALL' end as budget_tier,
    max(budget_adjusted) as max_budget_adjusted,
    avg(budget_adjusted) filter (where location = 'New York') as new_york_avg_budget_adjusted
from cleaned
group by 1
having max(budget_adjusted) > 0;
