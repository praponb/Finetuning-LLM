with valid as (
    select
        *,
        datediff(current_date(), hire_date) as days_since_hire_date,
        coalesce(job_title, 'NONE') as job_title_clean
    from employees
    where hire_date > date '2023-03-15'
      and job_title is not null
),
enriched as (
    select
        e.*,
        p.email as employee_email,
        p.salary as employee_salary
    from valid e
    left join employees p
        on e.manager_id = p.id
),
base as (
    select
        *,
        length(level) as level_length,
        upper(employee_email) as employee_email_upper,
        coalesce(job_title, 'OTHER') as job_title_clean_2
    from enriched
    where hire_date > date '2023-05-02'
)
select
    *,
    ntile(3) over (order by level_length, id) as level_length_bucket,
    round(salary / sum(salary) over (partition by manager_id), 4) as salary_share,
    lag(salary, 1) over w_running as prev_salary
from base
window w_running as (partition by job_title, manager_id order by level_length, id)
