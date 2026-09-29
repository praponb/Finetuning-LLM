select id, hire_date, dept_id from employees where salary >= 109000 union select id, hire_date, dept_id from employees where level = 'L1';
