select id, salary, is_remote
from employees
where salary >= (select max(salary) from employees)
