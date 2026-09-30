select
    id,
    hire_date,
    salary,
    sum(salary) over (order by hire_date, id rows between unbounded preceding and current row) running_salary
from employees
order by salary;
