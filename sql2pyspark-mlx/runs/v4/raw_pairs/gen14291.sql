with metrics as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        location,
        budget,
        lag(budget, 2) over (order by budget, id) as prev_budget,
        ntile(3) over (partition by dept_name order by budget desc, id) as budget_bucket,
        first_value(location, true) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) as first_known_location
    from departments
)
select
    id,
    dept_name,
    budget,
    prev_budget,
    budget_bucket,
    first_known_location
from metrics
