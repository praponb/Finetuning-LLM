select
    emp.id,
    emp.hire_date as employee_hire_date,
    mgr.job_title as manager_job_title,
    mgr.hire_date as manager_hire_date
from employees emp
left outer join employees mgr on emp.manager_id = mgr.id;
