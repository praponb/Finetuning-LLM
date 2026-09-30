with valid as (
    -- Filter the raw rows and derive helper columns
    select *, initcap(trim(dept_name)) as dept_name_title
    from departments
    where budget is not null
),
kept_keys as (
    -- Set difference on keys
    select location, id
    from valid
    except all
    select location, id
    from departments
    where budget < 1792000
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join kept_keys k on s.location = k.location and s.id = k.id
),
metrics as (
    -- Running and moving aggregates
    select
        dept_name,
        location,
        budget,
        cume_dist() over (partition by dept_name order by budget desc nulls first) as budget_cume_dist
    from verified
),
cleaned as (
    -- Basic cleansing and defaults
    select *, concat(location, ' ', dept_name) as location_dept_name
    from metrics
    where dept_name is not distinct from 'HR'
      and dept_name in ('Sales', 'Finance', 'Marketing')
),
enriched as (
    -- Running and moving aggregates
    select
        location,
        budget,
        location_dept_name,
        round(budget / sum(budget) over (partition by location), 4) as budget_share,
        percent_rank() over (partition by location order by budget nulls first) as budget_percent_rank,
        dense_rank() over (order by budget desc nulls first) as budget_dense_rank,
        percent_rank() over w_part as budget_percent_rank_2,
        avg(budget_cume_dist) over w_grp as location_avg_budget_cume_dist
    from cleaned
    window w_part as (order by budget desc nulls last), w_grp as (partition by location)
),
cleaned_departments as (
    -- Filter the raw rows and derive helper columns
    select
        location,
        budget,
        location_dept_name,
        budget_share,
        budget_percent_rank,
        budget_dense_rank,
        budget_percent_rank_2,
        budget_dense_rank - budget_percent_rank as budget_dense_rank_minus_budget_percent_rank,
        coalesce(location, 'N/A') as location_clean
    from enriched
    where location is not null
      and budget_dense_rank is null
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(budget) over (partition by location) as location_sum_budget,
        count(budget) over (partition by location) as location_count_budget,
        percent_rank() over (partition by location order by budget_dense_rank desc nulls first) as budget_dense_rank_percent_rank
    from cleaned_departments
)
select
    budget,
    budget_share,
    budget_percent_rank_2,
    location_clean,
    location_sum_budget,
    location_count_budget
from with_windows
