select
    e.id,
    e.is_remote as employee_is_remote,
    e.email as employee_email,
    m.level as manager_level,
    m.job_title as manager_job_title,
    m.salary as manager_salary
from employees e
inner join employees m on e.manager_id = m.id
where m.dept_id in (17, 21, 25, 31) and e.manager_id = 17
order by employee_email, id
limit 20
