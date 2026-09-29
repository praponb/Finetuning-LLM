with ranked as (
    -- Keep one row per key (latest first)
    select *
    from departments
    qualify row_number() over (partition by dept_name order by budget desc, id) = 1
),
ranked_departments as (
    -- Keep one row per key (latest first)
    select *
    from ranked
    qualify row_number() over (partition by location, dept_name order by budget desc nulls first, id) = 1
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        ntile(4) over w_running as budget_bucket,
        first_value(dept_name) ignore nulls over (partition by dept_name order by budget, id rows between unbounded preceding and current row) as first_known_dept_name
    from ranked_departments
    window w_running as (partition by location, dept_name order by budget, id)
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        location,
        budget,
        budget_bucket,
        first_known_dept_name,
        sum(budget) over (partition by dept_name order by budget, id rows between unbounded preceding and current row) as running_budget,
        last_value(budget, true) over (partition by location, dept_name order by budget, id rows between unbounded preceding and current row) as last_known_budget
    from enriched
)
-- Multi-level aggregation
select
    case when last_known_budget >= 3068000 then 'PREMIUM' else 'STANDARD' end as last_known_budget_tier,
    case when budget_bucket >= 7 then 'LARGE' else 'SMALL' end as budget_bucket_tier,
    sort_array(collect_set(location)) as location_list,
    stddev_samp(budget) as budget_stddev,
    min(budget_bucket) as min_budget_bucket,
    size(collect_set(location)) as num_locations,
    percentile_approx(running_budget, array(0.25, 0.5, 0.75)) as running_budget_quartiles
from metrics
group by case when last_known_budget >= 3068000 then 'PREMIUM' else 'STANDARD' end, case when budget_bucket >= 7 then 'LARGE' else 'SMALL' end
order by budget_stddev desc nulls first;
