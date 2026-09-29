with with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        hire_date,
        level,
        sum(salary) over (partition by level) as level_sum_salary,
        round(salary / sum(salary) over (partition by dept_id), 4) as salary_share,
        first_value(job_title) ignore nulls over (partition by manager_id order by salary, id rows between unbounded preceding and current row) as first_known_job_title
    from employees
),
ranked as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, rank() over (partition by manager_id, level order by hire_date desc nulls last) as rn
        from with_windows
    ) ranked
    where rn = 1
),
ranked_employees as (
    -- Deduplicate with ROW_NUMBER
    select
        id,
        full_name,
        dept_id,
        level,
        salary_share
    from ranked
    qualify rank() over (partition by dept_id order by level_sum_salary) = 1
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        sum(salary_share) over (partition by dept_id, level order by salary_share, id rows between unbounded preceding and current row) as cumulative_salary_share,
        sum(salary_share) over (partition by dept_id order by salary_share, id rows between unbounded preceding and current row) as running_salary_share,
        ntile(3) over (partition by level order by salary_share desc, id) as salary_share_bucket,
        avg(salary_share) over (order by salary_share, id rows between 3 preceding and current row) as salary_share_avg_3
    from ranked_employees
    where full_name is not null
),
kept_keys as (
    -- Set difference on keys
    select dept_id, level
    from metrics
    except all
    select dept_id, level
    from employees
    where is_remote
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    join kept_keys k on s.dept_id = k.dept_id and s.level = k.level
)
select
    full_name,
    salary_share,
    cumulative_salary_share,
    running_salary_share,
    salary_share_bucket,
    salary_share_avg_3,
    case when full_name rlike '^(Olivia Garcia|Noah Park)$' then 'CORE' when full_name rlike '^(Alice Wong|bob smith)$' then 'OTHER_ZONE' else 'GROUP_B' end as full_name_zone
from verified
order by salary_share;
