select
    e.id,
    e.level as employee_level,
    e.email as employee_email,
    e.is_remote as employee_is_remote,
    m.email as manager_email,
    m.job_title as manager_job_title,
    initcap(trim(e.email)) as email_title
from employees e
join employees m on e.manager_id = m.id
order by e.email desc, id
limit 3;
