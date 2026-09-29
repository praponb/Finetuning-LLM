select id, dept_id, level, hire_date, salary, salary - lag(salary, 1, 0) over (partition by dept_id, level order by hire_date, id) as salary_change from employees where manager_id is null
