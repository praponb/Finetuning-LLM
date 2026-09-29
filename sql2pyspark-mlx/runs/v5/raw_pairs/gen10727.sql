select
    dim.job_title as employee_job_title,
    dim.level as employee_level,
    s.level as employee_level,
    count(*) as num_rows,
    sum(s.salary) as total_salary
from employees s
inner join employees dim on s.manager_id = dim.id
group by rollup (dim.job_title, dim.level, s.level)
order by employee_level nulls first;
