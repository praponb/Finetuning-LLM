with valid as (
    select
        *,
        case when salary >= 134000 then 'High' when salary >= 72000 then 'Medium' else 'Low' end as salary_level,
        coalesce(level, 'OTHER') as resolved_level
    from employees
    where hire_date >= date '2023-02-16'
      and dept_id in (18, 22)
),
valid_employees as (
    select
        id,
        email,
        job_title,
        hire_date,
        is_remote,
        salary_level,
        coalesce(job_title, 'unassigned') as job_title_filled
    from valid
    where hire_date >= date '2023-06-24'
      and level is not null
),
ranked as (
    select *
    from (
        select *, row_number() over (partition by job_title order by hire_date desc, id) as row_num
        from valid_employees
    ) x
    where row_num = 1
),
valid_employees_2 as (
    select
        *,
        if(is_remote, 'Yes', 'No') as is_remote_label,
        dayofweek(hire_date) as hire_date_dayofweek,
        coalesce(job_title, 'OTHER') as job_title_clean
    from ranked
    where job_title is not null
),
deduped as (
    select
        id,
        job_title,
        job_title_clean
    from (
        select *, row_number() over (partition by job_title order by hire_date_dayofweek desc nulls first, id) as rn
        from valid_employees_2
    ) t
    where rn = 1
)
select
    id,
    job_title,
    job_title_clean,
    case when job_title rlike '^(Analyst|Manager)$' then 'GROUP_A' else 'EXTENDED' end as job_title_zone
from deduped;
