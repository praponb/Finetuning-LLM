select month(hire_date) as hire_month, manager_id, avg(salary) as avg_salary
from employees
group by cube(month(hire_date), manager_id)
order by hire_month desc;
