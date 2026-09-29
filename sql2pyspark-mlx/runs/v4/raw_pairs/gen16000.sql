select dept_name, avg(budget) as avg_budget, sum(case when dept_name = 'Sales' then 1 else 0 end) as sales_count
from departments
group by dept_name
order by dept_name;
