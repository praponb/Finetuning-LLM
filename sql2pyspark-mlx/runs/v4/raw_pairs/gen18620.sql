select employees.id, employees.manager_id
from employees
where employees.salary >= 145000 and employees.dept_id in (select id from departments where budget < 2655000);
