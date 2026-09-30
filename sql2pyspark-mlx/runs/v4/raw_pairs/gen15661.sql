select e.hire_date as employee_hire_date, e.dept_id as employee_dept_id, m.full_name as manager_full_name, m.email as manager_email from employees e left join employees m on e.manager_id = m.id;
