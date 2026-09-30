select max(salary) - min(salary) salary_range
from employees
order by salary_range desc;
