select
    job_title,
    sort_array(collect_set(job_title)) as job_title_list,
    max(hire_date) as latest_hire_date,
    avg(salary) as avg_salary
from employees
group by job_title
having avg(salary) >= 120000
order by latest_hire_date asc, job_title asc
limit 20
