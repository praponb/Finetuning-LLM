select count(*) as row_count, max(hire_date) as latest_hire_date, max(salary) - min(salary) as salary_range
from employees
where is_remote and level in ('L4', 'L3', 'L2')
order by salary_range desc, row_count desc;
