select id, manager_id, salary, avg(salary) over (partition by manager_id) as manager_id_avg_salary from employees where level in ('L3', 'L2') order by salary desc;
