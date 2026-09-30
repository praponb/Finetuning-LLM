select id, salary
from employees
where salary >= (select avg(salary) from employees where is_remote);
