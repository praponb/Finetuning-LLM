select job_title, manager_count
from (
    select job_title, count(case when job_title = 'Manager' then 1 end) as manager_count
    from employees
    where hire_date < '2023-08-15'
    group by job_title
) as x
where manager_count < 4
order by job_title, manager_count desc
