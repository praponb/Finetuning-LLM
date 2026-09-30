select
    id,
    salary,
    email,
    dept_id
from employees
where not (is_remote = true and salary <= 92000)
order by dept_id desc, id asc
limit 1;
