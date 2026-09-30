select emp.hire_date as employee_hire_date, mgr.dept_id as manager_dept_id, mgr.salary as manager_salary
from employees emp
left outer join employees mgr on emp.manager_id = mgr.id
where emp.manager_id is not null;
