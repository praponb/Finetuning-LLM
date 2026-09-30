select location, dept_name, count(budget) as budget_count
from departments
group by location, dept_name
order by location
