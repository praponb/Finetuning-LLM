with filtered as (
    select
        *,
        date_trunc('WEEK', hire_date) as hire_date_period,
        coalesce(level, 'UNKNOWN') as resolved_level
    from employees
    where is_remote
)
select *, row_number() over (partition by job_title, manager_id order by hire_date, id) as rn
from filtered
qualify rn = 1
