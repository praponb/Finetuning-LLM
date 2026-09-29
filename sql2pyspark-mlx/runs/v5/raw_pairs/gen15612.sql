select
    e.id,
    e.job_title as employee_job_title,
    e.dept_id as employee_dept_id,
    m.email as manager_email,
    m.level as manager_level
from employees as e
left outer join employees as m on e.manager_id = m.id
