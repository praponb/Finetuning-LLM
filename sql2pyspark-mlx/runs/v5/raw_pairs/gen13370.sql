with filtered as (
    select
        id,
        manager_id,
        job_title,
        hire_date,
        is_remote,
        level,
        datediff(current_date(), hire_date) as days_since_hire_date
    from employees
    where lower(trim(full_name)) = 'ivy chen'
)
select
    p.level as employee_level,
    p.job_title as employee_job_title,
    a.job_title as employee_job_title,
    count(*) as num_rows,
    avg(a.days_since_hire_date) as avg_days_since_hire_date
from filtered a
inner join employees p on a.manager_id = p.id
group by p.level, p.job_title, a.job_title
order by employee_job_title;
