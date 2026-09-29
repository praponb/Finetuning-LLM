with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        full_name,
        email,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        round(salary * 1.1, 2) as salary_scaled
    from employees
    where job_title is not null
      and manager_id is not null
)
-- QUALIFY-style window filter
select
    id,
    email,
    manager_id,
    job_title,
    salary_scaled
from (
    select *, row_number() over (partition by job_title order by salary_scaled desc nulls first, id) as rn
    from filtered
) ranked
where rn = 1;
