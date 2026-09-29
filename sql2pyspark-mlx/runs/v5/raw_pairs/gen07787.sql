with filtered as (
    select
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        substring(level, 1, 3) as level_prefix,
        coalesce(job_title, 'N/A') as resolved_job_title
    from employees
    where job_title is not null
      and hire_date > to_date('2023-05-15')
),
joined as (
    select
        src.id,
        src.email,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.is_remote,
        src.level,
        src.level_prefix,
        p.dept_name,
        p.budget,
        p.location
    from filtered src
    left join departments p
        on src.dept_id = p.id
    where p.location = 'New York'
),
filtered_employees as (
    select
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        level,
        level_prefix,
        budget,
        location,
        initcap(trim(dept_name)) as dept_name_title,
        coalesce(location, 'Unknown') as location_filled
    from joined
    where level is not null
),
with_dims as (
    select
        src.id,
        src.email,
        src.dept_id,
        src.manager_id,
        src.job_title,
        src.level_prefix,
        src.budget,
        src.location,
        d.level as employee_level,
        d.salary,
        d.email as employee_email
    from filtered_employees src
    left join employees d
        on src.manager_id <=> d.id
)
select
    dim.location as department_location,
    s.employee_level,
    count(*) as cnt,
    min(s.budget) as min_budget,
    round(sum(s.salary), 2) as total_salary,
    grouping_id() as grouping_level
from with_dims s
join departments dim on s.dept_id = dim.id
group by rollup (dim.location, s.employee_level)
order by total_salary desc;
