select
    emp.id,
    emp.email as employee_email,
    mgr.hire_date as manager_hire_date,
    round(emp.salary, 0) as salary_rounded
from employees emp
left join employees mgr on emp.manager_id = mgr.id
order by emp.id
