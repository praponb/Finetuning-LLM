select count(case when level = 'L1' then 1 end) as l1_count
from employees
where dept_id in (2, 27) and (dept_id in (6, 11) or salary >= 115000)
order by l1_count desc
