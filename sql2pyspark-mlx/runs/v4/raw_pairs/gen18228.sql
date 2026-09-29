with valid as (
    -- Keep recent, valid rows
    select
        *,
        length(location) as location_length,
        round(budget * 1.07, 2) as budget_adjusted
    from departments
    where location is not null
      and location in ('Bangkok', 'New York')
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        count(location_length) over (partition by dept_name) as dept_name_count_location_length,
        last_value(budget_adjusted) ignore nulls over (partition by dept_name, location order by budget asc, id asc rows between unbounded preceding and current row) as last_known_budget_adjusted
    from valid
),
valid_departments as (
    -- Basic cleansing and defaults
    select
        id,
        location,
        budget,
        location_length,
        budget_adjusted,
        last_known_budget_adjusted,
        coalesce(dept_name_count_location_length, 0) as dept_name_count_location_length_filled
    from with_windows
    where dept_name in ('HR', 'Legal', 'Marketing')
),
cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(dept_name_count_location_length_filled * 1.07, 2) as dept_name_count_location_length_filled_adjusted,
        coalesce(location, 'N/A') as resolved_location
    from valid_departments
    where location in ('London', 'Singapore', 'Tokyo')
),
scored as (
    -- Ranking and gap features
    select
        id,
        location,
        budget,
        location_length,
        budget_adjusted,
        last_known_budget_adjusted,
        dept_name_count_location_length_filled,
        dept_name_count_location_length_filled_adjusted,
        resolved_location,
        count(dept_name_count_location_length_filled) over (partition by location) as location_count_dept_name_count_location_length_filled,
        sum(dept_name_count_location_length_filled_adjusted) over (partition by location order by budget_adjusted asc, id asc rows between unbounded preceding and current row) as running_dept_name_count_location_length_filled_adjusted
    from cleaned
),
with_windows_departments as (
    -- Running and moving aggregates
    select
        id,
        location,
        budget,
        location_length,
        budget_adjusted,
        last_known_budget_adjusted,
        dept_name_count_location_length_filled,
        dept_name_count_location_length_filled_adjusted,
        resolved_location,
        location_count_dept_name_count_location_length_filled,
        running_dept_name_count_location_length_filled_adjusted,
        last_value(location) ignore nulls over w_ordered as last_known_location,
        sum(budget) over (partition by location order by dept_name_count_location_length_filled_adjusted asc, id asc rows between unbounded preceding and current row) as running_budget,
        sum(location_count_dept_name_count_location_length_filled) over w_ordered as running_location_count_dept_name_count_location_length_filled,
        lag(location_count_dept_name_count_location_length_filled, 1, 0) over w_hist as prev_location_count_dept_name_count_location_length_filled
    from scored
    window w_ordered as (partition by location order by budget asc, id asc rows between unbounded preceding and current row), w_hist as (order by running_dept_name_count_location_length_filled_adjusted asc, id asc)
),
metrics as (
    -- Ranking and gap features
    select
        *,
        rank() over (partition by location order by running_budget asc) as running_budget_rank,
        dense_rank() over (partition by location order by dept_name_count_location_length_filled_adjusted asc) as dept_name_count_location_length_filled_adjusted_dense_rank,
        row_number() over (partition by location order by running_location_count_dept_name_count_location_length_filled asc, id asc) as event_seq,
        count(budget) over (partition by location) as location_count_budget,
        sum(location_count_dept_name_count_location_length_filled) over (partition by location) as location_sum_location_count_dept_name_count_location_length_filled
    from with_windows_departments
)
-- Window metrics over several different partitions
select
    id,
    location,
    budget,
    location_length,
    budget_adjusted,
    last_known_budget_adjusted,
    dept_name_count_location_length_filled,
    dept_name_count_location_length_filled_adjusted,
    resolved_location,
    location_count_dept_name_count_location_length_filled,
    running_dept_name_count_location_length_filled_adjusted,
    last_known_location,
    running_budget,
    running_location_count_dept_name_count_location_length_filled,
    prev_location_count_dept_name_count_location_length_filled,
    running_budget_rank,
    dept_name_count_location_length_filled_adjusted_dense_rank,
    event_seq,
    location_count_budget,
    location_sum_location_count_dept_name_count_location_length_filled,
    lag(dept_name_count_location_length_filled_adjusted_dense_rank, 2, 0) over (partition by location order by dept_name_count_location_length_filled_adjusted asc, id asc) as prev_dept_name_count_location_length_filled_adjusted_dense_rank,
    percent_rank() over (partition by location order by location_sum_location_count_dept_name_count_location_length_filled asc nulls first) as location_sum_location_count_dept_name_count_location_length_filled_percent_rank
from metrics
