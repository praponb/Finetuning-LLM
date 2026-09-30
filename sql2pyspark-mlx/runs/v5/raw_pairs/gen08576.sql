with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        substring(dept_name, 1, 3) as dept_name_prefix,
        round(budget, 2) as budget_rounded
    from departments
    where location is not null
)
-- Window metrics over several different partitions
select
    id,
    dept_name_prefix,
    avg(budget_rounded) over w_grp as running_avg_budget_rounded,
    min(budget_rounded) over w_grp as running_min_budget_rounded,
    sum(budget_rounded) over w_grp as running_sum_budget_rounded,
    max(budget_rounded) over w_grp as running_max_budget_rounded
from cleaned
window w_grp as (partition by location order by budget, id rows between unbounded preceding and current row)
