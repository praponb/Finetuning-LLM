select
    dim.job_title as employee_job_title,
    dim.level as employee_level,
    s.job_title as employee_job_title,
    count(*) as rows_total,
    round(sum(s.salary), 2) as total_salary,
    grouping_id() as grouping_level
from employees s
inner join employees dim on s.manager_id = dim.id
group by rollup (dim.job_title, dim.level, s.job_title)
