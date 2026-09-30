with base as (
    -- Basic cleansing and defaults
    select *, concat_ws(',', location, dept_name) as location_dept_name_key
    from departments
    where location in ('Bangkok', 'Chiang Mai', 'Tokyo')
)
-- Running and moving aggregates
select
    id,
    location,
    budget,
    lag(budget) over w_grp as prev_budget,
    lead(budget) over w_grp as next_budget,
    row_number() over w_grp as event_seq
from base
window w_grp as (partition by location order by budget, id);
