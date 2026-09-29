select id, hire_date, email from employees where hire_date <= (select min(hire_date) from employees where dept_id = 11) order by email, id limit 50;
