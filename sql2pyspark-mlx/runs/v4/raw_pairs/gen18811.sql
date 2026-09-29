select
    id,
    manager_id,
    hire_date,
    dept_id,
    salary,
    rank() over (partition by manager_id order by hire_date) as rnk,
    sum(salary) over (partition by dept_id) as dept_id_sum_salary
from employees
where dept_id in (8, 18, 34, 37)
order by hire_date
