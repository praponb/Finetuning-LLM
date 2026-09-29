select quarter(hire_date) as hire_quarter, round(avg(salary), 2) as avg_salary
from employees
group by quarter(hire_date)
having round(avg(salary), 2) < 30
order by avg_salary desc
