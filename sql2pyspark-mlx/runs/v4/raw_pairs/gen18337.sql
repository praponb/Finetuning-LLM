select * from (select level, min(salary) as min_salary from employees where hire_date > '2023-10-01' group by level) as t where min_salary >= 125000;
