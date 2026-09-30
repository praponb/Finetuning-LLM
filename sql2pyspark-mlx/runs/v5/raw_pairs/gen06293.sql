with cleaned as (
    select
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        coalesce(email, 'Unknown') as email_filled,
        lower(full_name) as full_name_lower
    from employees
    where hire_date >= date '2023-04-07'
),
base as (
    select
        id,
        full_name,
        email,
        manager_id,
        salary,
        is_remote,
        level,
        email_filled,
        ceil(salary) as salary_whole
    from cleaned
    where manager_id is not null
),
base_employees as (
    select
        *,
        case when salary >= 128000 then 'High' when salary >= 53000 then 'Medium' else 'Low' end as salary_level,
        coalesce(level, 'N/A') as resolved_level
    from base
    where manager_id in (2, 19, 29, 36)
),
ranked as (
    select
        id,
        full_name,
        email,
        manager_id,
        salary,
        is_remote,
        level,
        email_filled,
        salary_whole,
        salary_level
    from base_employees
    qualify row_number() over (partition by manager_id order by salary desc, id) = 1
)
select
    e.level as employee_level,
    e.job_title,
    count(*) as num_rows,
    sum(a.salary_whole) as total_salary_whole
from ranked a
inner join employees e on a.manager_id = e.id
group by e.level, e.job_title
order by job_title desc
