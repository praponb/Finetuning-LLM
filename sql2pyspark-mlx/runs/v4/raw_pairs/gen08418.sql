with with_dims as (
    -- Attach departments attributes
    select e.*, d.dept_name
    from employees e
    inner join departments d
        on e.dept_id = d.id
),
with_dims_employees as (
    -- Attach departments attributes
    select src.*, p.location
    from with_dims src
    left join departments p
        on src.dept_id = p.id
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        trim(job_title) as clean_job_title,
        date_format(hire_date, 'yyyy-MM') as hire_date_label,
        coalesce(job_title, 'DIRECT') as resolved_job_title
    from with_dims_employees
    where hire_date < '2025-07-22'
)
-- Native SQL PIVOT
select *
from (
    select
        dept_name,
        location,
        salary
    from valid
) src
pivot (
    sum(salary) as value_sum,
    count(1) as cnt
    for location in ('Tokyo' as tokyo, 'Bangkok' as bangkok, 'New York' as new_york)
);
