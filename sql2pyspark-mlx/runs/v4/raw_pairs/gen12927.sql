select
    id,
    job_title,
    hire_date,
    salary,
    count(*) over (partition by job_title) job_title_count,
    sum(salary) over (partition by job_title order by hire_date, id rows between unbounded preceding and current row) running_salary
from employees
where job_title = 'Designer'
order by running_salary desc;
