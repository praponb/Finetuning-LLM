select job_title
from employees
except distinct
select job_title
from employees
where job_title in ('Manager', 'Analyst');
