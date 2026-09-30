with Src as (
    select *, if(is_remote, 'Yes', 'No') as is_remote_label
    from employees
    where dept_id = 6
),
employees_stats as (
    select level, max(salary) as max_salary, round(avg(salary), 2) as avg_salary
    from Src
    group by level
)
select level, max_salary, avg_salary
from employees_stats
where avg_salary < 40
order by avg_salary, level;
