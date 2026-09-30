with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        level,
        case when is_remote then 'Yes' else 'No' end as is_remote_label,
        date_trunc('MONTH', hire_date) as hire_date_period,
        coalesce(job_title, 'UNKNOWN') as job_title_filled
    from employees
    where hire_date >= date '2023-06-22'
)
select job_title, level
from valid
except distinct
select job_title, level
from employees
where lower(trim(full_name)) = 'jack taylor'
