select is_remote, level, sum(salary) total_salary
from employees
where dept_id in (22, 28) and length(full_name) > 10
group by is_remote, level
having total_salary >= 788000
order by level desc
