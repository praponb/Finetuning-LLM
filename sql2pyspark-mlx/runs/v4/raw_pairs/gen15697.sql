select id, dept_id, hire_date, salary, sum(salary) over (partition by dept_id order by hire_date, id) as running_salary from employees where manager_id is not null;
