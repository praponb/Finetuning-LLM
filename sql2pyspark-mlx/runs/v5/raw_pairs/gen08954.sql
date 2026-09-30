with filtered_employees as (
    select *, trim(full_name) as clean_full_name
    from employees
    where dept_id in (5, 6, 32) or full_name like 'O%'
),
employees_stats as (
    select is_remote, sum(case when job_title = 'Director' then salary else 0 end) as director_salary, max(hire_date) as latest_hire_date
    from filtered_employees
    group by is_remote
)
select *
from employees_stats
where director_salary >= 31
order by director_salary desc, latest_hire_date, is_remote
limit 5;
