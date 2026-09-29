select
    id,
    budget,
    round(budget, 1) as budget_rounded,
    upper(dept_name) as dept_name_upper
from departments
where dept_name in ('Legal', 'Sales', 'HR')
order by budget_rounded desc
