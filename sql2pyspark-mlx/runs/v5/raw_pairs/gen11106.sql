select
    id,
    budget,
    dept_name,
    round(budget * 0.9, 2) as budget_scaled
from departments
where budget is null
