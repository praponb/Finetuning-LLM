select
    emp.id,
    emp.level as employee_level,
    emp.email as employee_email,
    emp.manager_id as employee_manager_id,
    mgr.job_title as manager_job_title,
    mgr.email as manager_email,
    mgr.manager_id as manager_manager_id,
    cast(mgr.salary as int) as salary_int
from employees as emp
inner join employees as mgr on emp.manager_id = mgr.id
where mgr.level != 'L4' or emp.level = 'L3'
order by manager_manager_id, emp.level
