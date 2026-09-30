with metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(budget, 2) over (partition by dept_name order by budget asc, id asc) as prev_budget,
        dense_rank() over (partition by location, dept_name order by budget asc nulls first) as budget_dense_rank,
        ntile(3) over (order by budget asc, id asc) as budget_bucket
    from departments
)
select
    id,
    dept_name,
    location,
    budget,
    prev_budget,
    budget_dense_rank,
    case when location rlike '^(London|Berlin)$' then 'GROUP_B' else 'CORE' end as location_zone
from metrics
