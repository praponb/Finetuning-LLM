select year(hire_date) hire_year, sum(salary) total_salary, round(sum(salary) * 100.0 / sum(sum(salary)) over (), 2) as pct_of_total
from employees
where salary between 109000 and 148000
group by year(hire_date)
order by total_salary desc;
