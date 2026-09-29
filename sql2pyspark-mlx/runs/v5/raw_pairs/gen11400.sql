select job_title, min(hire_date) as min_hire_date
from employees
group by job_title
order by min_hire_date;
