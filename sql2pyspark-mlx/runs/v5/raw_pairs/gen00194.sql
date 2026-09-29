select emp.id, emp.job_title as employee_job_title, mgr.level as manager_level
from employees emp
left join employees mgr on emp.manager_id = mgr.id
order by mgr.level desc;
