select
    e.id,
    e.is_remote as employee_is_remote,
    e.email as employee_email,
    e.dept_id as employee_dept_id,
    m.job_title as manager_job_title,
    m.salary as manager_salary
from employees as e
left outer join employees as m on e.manager_id = m.id
where e.manager_id in (3, 10, 23) and m.email is not null
order by e.email;
