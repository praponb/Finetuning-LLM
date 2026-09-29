select
    id,
    salary,
    dept_id,
    full_name
from employees
where salary <= (select min(salary) from employees)
order by full_name, salary desc;
