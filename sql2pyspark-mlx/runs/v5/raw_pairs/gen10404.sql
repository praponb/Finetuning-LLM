select
    hire_date,
    is_remote,
    ceil(salary) as salary_whole,
    cast(is_remote as int) as is_remote_flag
from employees
where salary > 105000
order by hire_date, is_remote_flag desc;
