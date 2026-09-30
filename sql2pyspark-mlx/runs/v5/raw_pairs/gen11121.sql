select
    emp.email as employee_email,
    mgr.is_remote as manager_is_remote,
    mgr.full_name as manager_full_name,
    mgr.job_title as manager_job_title,
    initcap(trim(mgr.full_name)) as full_name_title
from employees emp
inner join employees mgr on emp.manager_id = mgr.id
where mgr.salary <= 69000
order by full_name_title desc;
