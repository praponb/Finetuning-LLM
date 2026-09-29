select
    id,
    level,
    hire_date,
    manager_id,
    salary,
    rank() over (partition by level order by hire_date desc) as hire_date_rank,
    sum(salary) over (partition by manager_id order by hire_date, id rows between unbounded preceding and current row) as running_salary
from employees
where level in ('L4', 'L5', 'L2');
