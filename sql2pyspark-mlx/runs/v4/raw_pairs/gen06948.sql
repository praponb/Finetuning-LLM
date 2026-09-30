select
    job_title,
    sum(salary) as total_salary,
    count(*) as cnt,
    round(sum(salary) * 100.0 / sum(sum(salary)) over (), 2) as pct_of_total
from employees
group by job_title;
