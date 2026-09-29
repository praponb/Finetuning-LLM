with base as (
    select *, trim(job_title) as clean_job_title
    from employees
    where hire_date >= date '2023-05-04'
)
select
    manager_id,
    sum(case when job_title = 'Analyst' then salary else 0 end) as analyst_salary,
    size(collect_set(job_title)) as num_job_titles,
    count_if(job_title = 'Analyst') as analyst_events,
    size(collect_set(job_title)) as num_job_titles_2,
    min(salary) as min_salary,
    count_if(job_title = 'Intern') as intern_events
from base
group by manager_id
having min(salary) >= 0
