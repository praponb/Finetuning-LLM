select
    id,
    job_title,
    hire_date,
    salary,
    ntile(10) over (partition by job_title order by hire_date desc, id) as hire_date_ntile,
    sum(salary) over (partition by job_title) as job_title_sum_salary
from employees;
