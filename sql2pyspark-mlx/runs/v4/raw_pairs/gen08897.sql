select is_remote, sum(case when level = 'L3' then 1 else 0 end) l3_count, min(salary) min_salary
from employees
group by is_remote
having count(*) < 1
order by l3_count desc
