select id, hire_date, level
from employees
where job_title in ('Engineer', 'Intern', 'Designer')
union all
select id, hire_date, level
from employees
where salary between 60000 and 125000
