with valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when location in ('Tokyo', 'Bangkok') then 'Priority' else 'Other' end as location_group,
        case when location in ('Singapore', 'Bangkok') then 'Priority' else 'Other' end as location_group_2
    from departments
    where location is not null
      and dept_name <> 'HR'
)
-- Running and moving aggregates
select
    id,
    location,
    avg(budget) over (partition by dept_name, location order by budget, id rows between 3 preceding and 1 preceding) as budget_avg_3_prior,
    lag(budget, 1, 0) over (partition by location, dept_name order by budget, id) as prev_budget
from valid;
