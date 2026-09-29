select is_remote, sum(salary) as total_salary, dense_rank() over (order by sum(salary) desc) as rnk
from employees
group by is_remote
