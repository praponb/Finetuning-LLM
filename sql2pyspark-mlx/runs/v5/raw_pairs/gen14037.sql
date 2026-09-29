select id, salary from employees where salary >= (select max(salary) from employees)
