with base as (
    -- Keep recent, valid rows
    select
        *,
        case when job_title in ('Manager', 'Analyst') then 'Core' else 'Other' end as job_title_group,
        case when is_remote then 'Yes' else 'No' end as is_remote_label
    from employees
    where hire_date > to_date('2023-02-10')
      and full_name like '%smit%'
),
valid as (
    -- Filter the raw rows and derive helper columns
    select *, concat_ws('/', email, level) as email_level_key
    from base
    where email like '%@GMAIL.COM'
),
metrics as (
    -- Running and moving aggregates
    select
        *,
        lag(salary) over w_grp as prev_salary,
        lead(salary) over w_grp as next_salary,
        row_number() over w_grp as event_seq
    from valid
    where job_title = 'Intern'
    window w_grp as (partition by dept_id order by hire_date asc, id asc)
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        job_title_group,
        is_remote_label,
        email_level_key,
        prev_salary,
        next_salary,
        event_seq,
        date_trunc('month', hire_date) as hire_date_period,
        coalesce(level, 'OTHER') as level_clean
    from metrics
    where hire_date > to_date('2023-07-09')
)
-- Window metrics over several different partitions
select
    id,
    full_name,
    manager_id,
    job_title,
    is_remote,
    job_title_group,
    email_level_key,
    next_salary,
    lag(next_salary) over (partition by job_title order by next_salary asc, id asc) as prev_next_salary,
    lead(next_salary) over (partition by job_title order by next_salary asc, id asc) as next_next_salary,
    lead(event_seq) over (partition by job_title order by next_salary asc, id asc) as next_event_seq,
    row_number() over (partition by job_title order by next_salary asc, id asc) as row_seq
from cleaned;
