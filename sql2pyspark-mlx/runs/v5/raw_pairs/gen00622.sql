with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat_ws('/', dept_name, location) as dept_name_location_key,
        coalesce(dept_name, 'OTHER') as dept_name_filled
    from departments
    where dept_name is not null
),
metrics as (
    -- Ranking and gap features
    select
        id,
        dept_name,
        location,
        dept_name_location_key,
        dept_name_filled,
        max(budget) over (partition by location order by budget asc, id asc rows between 2 preceding and current row) as last3_max_budget,
        avg(budget) over (partition by location order by budget asc, id asc rows between 2 preceding and current row) as last3_avg_budget,
        count(budget) over (partition by location order by budget asc, id asc rows between 2 preceding and current row) as last3_count_budget,
        lag(budget, 2) over (order by budget asc, id asc) as prev_budget
    from filtered
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from employees c
            where c.dept_id = s.id
              and c.manager_id in (6, 18, 29)
        )
),
enriched as (
    -- Ranking and gap features
    select
        *,
        count(last3_max_budget) over w_recent as last3_count_last3_max_budget,
        avg(last3_max_budget) over w_recent as last3_avg_last3_max_budget,
        max(last3_max_budget) over w_recent as last3_max_last3_max_budget
    from not_flagged
    window w_recent as (partition by location, dept_name order by prev_budget asc, id asc rows between 2 preceding and current row)
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and lower(trim(ch.full_name)) = 'ivy chen'
        )
)
-- Running and moving aggregates
select
    *,
    ntile(4) over (partition by location order by last3_avg_last3_max_budget asc, id asc) as last3_avg_last3_max_budget_bucket,
    avg(last3_avg_last3_max_budget) over (order by last3_avg_last3_max_budget asc, id asc rows between 5 preceding and 1 preceding) as last3_avg_last3_max_budget_avg_5_prior
from kept
where last3_max_last3_max_budget > 2690000;
