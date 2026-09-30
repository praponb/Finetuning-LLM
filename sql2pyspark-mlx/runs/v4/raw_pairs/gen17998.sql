select dept_name, max(budget) - min(budget) as budget_range, avg(budget) as avg_budget
from departments
group by dept_name
having avg(budget) < 3185000
order by avg_budget
