select
    job_title,
    level,
    dept_id,
    add_months(hire_date, 1) hire_date_plus_months
from employees
where full_name like '%Car%' and dept_id in (19, 27, 39)
order by level asc
