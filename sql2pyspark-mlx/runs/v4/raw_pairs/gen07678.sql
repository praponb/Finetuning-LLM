with filtered_employees as (
    select *
    from employees
    where dept_id in (17, 24)
),
job_title_summary as (
    select job_title, level, avg(salary) as avg_salary
    from filtered_employees
    group by job_title, level
)
select job_title, level, avg_salary
from job_title_summary
order by job_title, avg_salary desc, level
limit 10;
