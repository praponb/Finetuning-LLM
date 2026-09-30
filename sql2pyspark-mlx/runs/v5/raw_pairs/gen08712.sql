select
    dim.level as employee_level,
    dim.job_title as employee_job_title,
    s.level as employee_level,
    count(*) as cnt,
    avg(s.salary) as avg_salary,
    grouping_id() as grouping_level
from employees s
inner join employees dim on s.manager_id = dim.id
group by grouping sets ((dim.level, dim.job_title, s.level), (dim.level, dim.job_title), (dim.level), ())
