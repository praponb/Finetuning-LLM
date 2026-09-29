with with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(budget) over w_ordered as last3_avg_budget,
        max(budget) over w_ordered as last3_max_budget,
        count(budget) over w_ordered as last3_count_budget,
        sum(budget) over w_ordered as last3_sum_budget,
        lag(budget, 2, 0) over (partition by dept_name order by budget, id) as prev_budget
    from departments
    where budget >= 1518000
    window w_ordered as (partition by dept_name, location order by budget, id rows between 2 preceding and current row)
)
-- Grouped metrics
select
    location,
    case when last3_avg_budget >= 3111000 then 'PREMIUM' else 'STANDARD' end as last3_avg_budget_tier,
    count(*) filter (where dept_name = 'HR') as hr_rows,
    percentile_approx(last3_max_budget, 0.95) as last3_max_budget_p95,
    percentile_approx(last3_sum_budget, 0.9) as last3_sum_budget_p90
from with_windows
group by rollup (location, case when last3_avg_budget >= 3111000 then 'PREMIUM' else 'STANDARD' end)
