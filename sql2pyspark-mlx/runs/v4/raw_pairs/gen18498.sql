select level, dept_id
from employees
where salary > 82000
union
select level, dept_id
from employees
where full_name is not null;
