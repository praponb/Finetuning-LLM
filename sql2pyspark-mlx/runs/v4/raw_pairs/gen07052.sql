select e.full_name as employee_full_name, e.dept_id as employee_dept_id, m.is_remote as manager_is_remote from employees e left outer join employees m on e.manager_id = m.id
