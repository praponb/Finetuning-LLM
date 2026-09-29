select level, month(hire_date) as hire_month, sum(salary) as total_salary
from employees
group by 1, 2;
