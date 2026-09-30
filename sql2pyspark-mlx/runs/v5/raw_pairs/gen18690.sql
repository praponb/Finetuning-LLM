select
    e.id,
    e.email as employee_email,
    m.manager_id as manager_manager_id,
    m.job_title as manager_job_title
from employees e
left join employees m on e.manager_id = m.id
where length(e.email) >= 6
order by id desc, manager_manager_id desc;
