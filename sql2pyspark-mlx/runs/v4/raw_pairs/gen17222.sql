with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        cast(budget as int) as budget_int,
        coalesce(budget, 0) as budget_filled,
        coalesce(location, 'N/A') as location_filled
    from departments
    where location is not null
)
-- Running and moving aggregates
select
    id,
    dept_name,
    location,
    budget,
    budget_int,
    budget_filled,
    location_filled,
    avg(budget_int) over (partition by dept_name order by budget_int, id rows between 2 preceding and current row) as budget_int_avg_2,
    sum(budget) over (partition by dept_name order by budget_filled, id rows between unbounded preceding and current row) as cumulative_budget,
    first_value(budget_int) ignore nulls over (order by budget_filled, id rows between unbounded preceding and current row) as first_known_budget_int,
    lag(budget_int, 2, 0) over (partition by dept_name order by budget_filled, id) as prev_budget_int,
    ntile(3) over (partition by dept_name order by budget desc, id) as budget_bucket
from cleaned;
