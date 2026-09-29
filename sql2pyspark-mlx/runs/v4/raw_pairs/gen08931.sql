select emp.full_name as employee_full_name, emp.hire_date as employee_hire_date, mgr.dept_id as manager_dept_id
from employees emp
left outer join employees mgr on emp.manager_id = mgr.id
where mgr.salary < 85000
order by employee_full_name
