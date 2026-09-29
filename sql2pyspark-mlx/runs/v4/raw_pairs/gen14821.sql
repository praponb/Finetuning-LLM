select is_remote, sum(salary) as total_salary, round(sum(salary) * 100.0 / sum(sum(salary)) over (), 2) as pct_of_total
from employees
group by is_remote
order by pct_of_total desc, is_remote asc
limit 50;
