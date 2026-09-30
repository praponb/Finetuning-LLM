select id, level, hire_date, salary, lead(salary, 1) over (partition by level order by hire_date, id) as next_salary from employees order by level
