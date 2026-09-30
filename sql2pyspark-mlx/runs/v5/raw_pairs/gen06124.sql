with valid as (
    -- Basic cleansing and defaults
    select
        *,
        case when location in ('Berlin', 'New York') then 'Priority' else 'Other' end as location_group,
        initcap(trim(dept_name)) as dept_name_title,
        coalesce(location, 'OTHER') as location_filled
    from departments
    where budget is null
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from valid
    qualify rank() over (partition by dept_name order by budget desc) = 1
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from employees ch
            where ch.dept_id = s.id
              and ch.salary < 145000
        )
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        count(budget) over (partition by location order by budget, id rows between unbounded preceding and current row) as running_count_budget,
        max(budget) over (partition by location order by budget, id rows between unbounded preceding and current row) as running_max_budget,
        min(budget) over (partition by location order by budget, id rows between unbounded preceding and current row) as running_min_budget
    from not_flagged
),
latest_match as (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        s.dept_name_title,
        s.location_filled,
        s.running_count_budget,
        s.running_max_budget,
        s.running_min_budget,
        latest.employee_salary,
        latest.job_title
    from metrics s
    left join lateral (
            select c.salary as employee_salary, c.job_title
            from employees c
            where c.dept_id = s.id
            order by c.salary desc, c.id desc
            limit 1
        ) latest on true
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        dept_name,
        location,
        budget,
        dept_name_title,
        location_filled,
        running_count_budget,
        job_title
    from latest_match
    qualify row_number() over (partition by location order by budget, id) = 1
)
-- Grouped metrics
select
    case when budget >= 3189000 then 'PREMIUM' else 'STANDARD' end as budget_tier,
    max(running_count_budget) filter (where dept_name = 'Marketing') as marketing_max_running_count_budget,
    count(distinct dept_name) as distinct_dept_names
from deduped
group by case when budget >= 3189000 then 'PREMIUM' else 'STANDARD' end
having marketing_max_running_count_budget > 0
order by distinct_dept_names
