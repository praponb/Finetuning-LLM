select
    is_remote,
    job_title,
    count(case when job_title = 'Designer' then 1 end) as designer_count,
    round(avg(salary), 2) as avg_salary,
    count(distinct salary) as unique_levels
from employees
where hire_date < '2023-03-01' or job_title = 'Intern'
group by is_remote, job_title
