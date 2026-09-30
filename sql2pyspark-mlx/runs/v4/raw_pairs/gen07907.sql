with valid as (
    -- Basic cleansing and defaults
    select *, upper(full_name) as full_name_upper
    from employees
    where hire_date between '2023-10-01' and '2024-12-01'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(4) over w_running as salary_bucket,
        sum(salary) over w_recent as cumulative_salary
    from valid
    window w_running as (order by salary, id), w_recent as (partition by job_title, manager_id order by salary, id rows between unbounded preceding and current row)
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        full_name_upper,
        salary_bucket,
        cumulative_salary,
        case when job_title in ('Intern', 'Analyst') then 'Tier 1' else 'Other' end as job_title_group,
        cast(is_remote as int) as is_remote_flag,
        coalesce(level, 'NONE') as level_clean
    from enriched
    where hire_date >= to_date('2023-10-16')
      and job_title is not null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        level,
        full_name_upper,
        salary_bucket,
        cumulative_salary,
        job_title_group,
        is_remote_flag,
        level_clean,
        lead(salary_bucket, 1) over (partition by manager_id order by is_remote_flag, id) as next_salary_bucket,
        row_number() over (order by salary desc, id) as event_seq
    from base
),
remaining_keys as (
    -- Set difference on keys
    select manager_id, level
    from with_windows
    except distinct
    select manager_id, level
    from employees
    where dept_id in (5, 8, 16, 30)
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from with_windows s
    join remaining_keys k on s.manager_id = k.manager_id and s.level = k.level
),
base_employees as (
    -- Keep recent, valid rows
    select
        id,
        email,
        salary,
        is_remote,
        level,
        full_name_upper,
        cumulative_salary,
        is_remote_flag,
        level_clean,
        next_salary_bucket,
        event_seq,
        round(is_remote_flag / salary_bucket, 2) as is_remote_flag_per_salary_bucket
    from kept_rows
    where job_title = 'Director'
)
select
    email,
    full_name_upper,
    is_remote_flag,
    next_salary_bucket,
    event_seq,
    is_remote_flag_per_salary_bucket
from base_employees
order by next_salary_bucket desc nulls first;
