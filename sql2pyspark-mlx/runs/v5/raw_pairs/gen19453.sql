with cleaned as (
    -- Keep recent, valid rows
    select
        *,
        round(budget * 0.9, 2) as budget_adjusted,
        trim(dept_name) as clean_dept_name
    from departments
    where budget <= 2631000
),
matched_keys as (
    -- Set intersection on keys
    select location, id
    from cleaned
    intersect
    select location, id
    from departments
    where dept_name != 'HR'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.location = k.location and s.id = k.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from reconciled s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.level != 'L5'
        )
),
scored as (
    -- Running and moving aggregates
    select
        *,
        round(budget / sum(budget) over (partition by dept_name, location), 4) as budget_share,
        max(budget_adjusted) over (partition by dept_name) as dept_name_max_budget_adjusted,
        max(budget) over w_part as location_max_budget
    from kept
    where location in ('Tokyo', 'Chiang Mai')
    window w_part as (partition by location)
),
scored_departments as (
    -- Running and moving aggregates
    select
        *,
        min(dept_name_max_budget_adjusted) over (partition by location) as location_min_dept_name_max_budget_adjusted,
        round(budget / sum(budget) over (partition by location), 4) as budget_share_2,
        dense_rank() over (partition by dept_name order by budget desc nulls last) as budget_dense_rank
    from scored
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        dept_name,
        budget_adjusted,
        budget_share,
        location_max_budget,
        location_min_dept_name_max_budget_adjusted,
        max(budget_share) over (partition by dept_name, location) as dept_name_max_budget_share,
        rank() over w_grp as budget_adjusted_rank,
        count(budget_adjusted) over (partition by location) as location_count_budget_adjusted,
        cume_dist() over (order by dept_name_max_budget_adjusted desc nulls last) as dept_name_max_budget_adjusted_cume_dist,
        rank() over (partition by dept_name order by location_min_dept_name_max_budget_adjusted desc nulls first) as location_min_dept_name_max_budget_adjusted_rank
    from scored_departments
    window w_grp as (order by budget_adjusted nulls last)
)
-- Multi-level aggregation
select
    case when location_min_dept_name_max_budget_adjusted_rank >= 8 then 'PREMIUM' else 'STANDARD' end as location_min_dept_name_max_budget_adjusted_rank_tier,
    dept_name,
    round(avg(dept_name_max_budget_share), 2) as avg_dept_name_max_budget_share,
    size(collect_set(dept_name)) as num_dept_names,
    min(location_min_dept_name_max_budget_adjusted) as min_location_min_dept_name_max_budget_adjusted,
    grouping_id() as grouping_id
from metrics
group by grouping sets ((case when location_min_dept_name_max_budget_adjusted_rank >= 8 then 'PREMIUM' else 'STANDARD' end, dept_name), (case when location_min_dept_name_max_budget_adjusted_rank >= 8 then 'PREMIUM' else 'STANDARD' end), ())
having min(location_min_dept_name_max_budget_adjusted) > 0;
