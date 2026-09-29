select
    job_title,
    date_trunc('MONTH', hire_date) as hire_month,
    count(case when level = 'L3' then 1 end) as l3_count,
    count(case when job_title = 'Director' then 1 end) as director_count
from employees
where hire_date >= '2024-09-10' and is_remote
group by job_title, hire_month
order by job_title;
