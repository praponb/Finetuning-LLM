with deduped as (
    select *
    from (
        select *, row_number() over (partition by manager_id order by salary desc, id) as rank_in_group
        from employees
    ) x
    where rank_in_group = 1
)
select
    id,
    full_name,
    dept_id,
    manager_id,
    job_title,
    hire_date,
    is_remote,
    level,
    rank_in_group,
    case when level rlike '^(L3|L5)$' then 'CORE' when level rlike '^(L1|L2)$' then 'EXTENDED' else 'OTHER_ZONE' end as level_zone
from deduped
order by job_title desc, manager_id desc;
