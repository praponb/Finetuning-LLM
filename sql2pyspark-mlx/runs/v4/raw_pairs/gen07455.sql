select year(hire_date) as hire_year, sum(salary) as total_salary, sum(sum(salary)) over (order by year(hire_date)) as cumulative_salary
from employees
group by year(hire_date)
