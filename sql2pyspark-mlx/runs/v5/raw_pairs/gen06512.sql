with valid as (
    select *, datediff(current_date(), hire_date) as days_since_hire_date
    from employees
    where hire_date > date '2023-04-04'
)
select
    e.id,
    e.full_name,
    e.email,
    e.dept_id,
    e.manager_id,
    e.salary,
    e.is_remote,
    e.level,
    e.days_since_hire_date,
    d.job_title as employee_job_title
from valid e
left join employees d
    on e.manager_id = d.id
where d.hire_date between '2024-04-23' and '2024-07-07';
