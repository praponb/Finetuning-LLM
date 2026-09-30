with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        manager_id,
        hire_date,
        is_remote,
        level,
        case when job_title in ('Analyst', 'Manager') then 'Group A' else 'Other' end as job_title_group,
        case when job_title = 'Analyst' then 1 else 0 end as is_analyst,
        coalesce(level, 'NONE') as level_filled
    from employees
    where hire_date > date '2023-04-18'
      and manager_id is not null
)
-- Keep one row per key (latest first)
select *, row_number() over (partition by level, manager_id order by is_analyst asc nulls first, id asc) as latest_rank
from filtered
qualify latest_rank = 1;
