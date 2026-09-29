select date_trunc('MONTH', hire_date) as hire_month, avg(salary) as avg_salary
from employees
where hire_date >= '2025-02-08'
group by date_trunc('MONTH', hire_date)
order by avg_salary desc;
