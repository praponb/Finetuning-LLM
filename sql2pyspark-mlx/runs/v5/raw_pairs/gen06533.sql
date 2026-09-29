select employees.id, employees.email, employees.salary from employees where employees.dept_id not in (select id from departments where budget > 1814000)
