with base as (
    -- Keep recent, valid rows
    select
        id,
        full_name,
        email,
        dept_id,
        job_title,
        hire_date,
        is_remote,
        level,
        case when is_remote then 'Yes' else 'No' end as is_remote_label,
        coalesce(job_title, 'N/A') as job_title_filled
    from employees
    where hire_date > date '2023-02-28'
      and manager_id is not null
)
-- Attach departments attributes
select src.*, b.budget
from base src
left join departments b
    on src.dept_id = b.id;
