select
    hire_date,
    is_remote,
    email,
    concat(job_title, ' ', full_name) as job_title_full_name
from employees
where job_title in ('Intern', 'Engineer')
