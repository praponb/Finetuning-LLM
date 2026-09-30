select dept_name, count(budget) as budget_count
from departments
where budget <= 3332000
group by dept_name
order by budget_count desc, dept_name
limit 20;
