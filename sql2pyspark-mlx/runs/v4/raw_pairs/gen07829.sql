select
    e.job_title as employee_job_title,
    e.is_remote as employee_is_remote,
    m.email as manager_email,
    m.salary as manager_salary
from employees e
left join employees m on e.manager_id = m.id
where m.manager_id is not null and not m.is_remote
order by manager_salary desc
