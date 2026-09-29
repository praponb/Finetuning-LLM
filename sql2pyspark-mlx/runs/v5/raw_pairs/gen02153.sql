select
    id,
    full_name,
    email,
    job_title,
    salary,
    hire_date,
    level,
    row_number() over (order by salary, id) as event_seq,
    lag(salary, 1) over (partition by job_title, level order by hire_date, id) as prev_salary,
    min(salary) over (partition by dept_id) as dept_id_min_salary
from employees
