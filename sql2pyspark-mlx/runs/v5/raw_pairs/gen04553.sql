select is_remote, max(salary) as max_salary from employees group by is_remote having max(salary) < 94000;
