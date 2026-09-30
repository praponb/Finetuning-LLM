select id, manager_id, salary, avg(salary) over (partition by manager_id) manager_id_avg_salary from employees where dept_id = 17
