select avg(budget) as avg_budget, sum(case when dept_name = 'Marketing' then 1 else 0 end) as marketing_count
from departments
where budget between 2092000 and 2495000 and (budget >= 1946000 or dept_name = 'Legal')
order by avg_budget desc;
