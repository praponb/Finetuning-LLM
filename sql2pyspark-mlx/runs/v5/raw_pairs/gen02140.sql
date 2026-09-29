select dept_name, sum(budget) as total_budget, round(sum(budget) * 100.0 / sum(sum(budget)) over (), 2) as pct_of_total
from departments
group by dept_name
order by total_budget, dept_name
limit 3;
