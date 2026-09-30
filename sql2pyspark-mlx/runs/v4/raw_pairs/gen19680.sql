select
    emp.id,
    emp.level as employee_level,
    emp.email as employee_email,
    mgr.job_title as manager_job_title,
    mgr.email as manager_email,
    cast(mgr.salary as int) as salary_int
from employees emp
left join employees mgr on emp.manager_id = mgr.id
order by mgr.email desc;
