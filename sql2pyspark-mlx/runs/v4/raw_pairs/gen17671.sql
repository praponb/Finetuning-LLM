select emp.full_name as employee_full_name, mgr.manager_id as manager_manager_id from employees emp inner join employees mgr on emp.manager_id = mgr.id where emp.manager_id = 5
