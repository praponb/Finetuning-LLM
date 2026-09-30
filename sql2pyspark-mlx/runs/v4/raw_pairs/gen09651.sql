select emp.id, emp.salary as employee_salary, mgr.hire_date as manager_hire_date from employees emp left join employees mgr on emp.manager_id = mgr.id
