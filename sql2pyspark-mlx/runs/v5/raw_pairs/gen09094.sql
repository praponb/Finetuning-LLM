select id, hire_date, salary from employees where hire_date >= (select max(hire_date) from employees);
