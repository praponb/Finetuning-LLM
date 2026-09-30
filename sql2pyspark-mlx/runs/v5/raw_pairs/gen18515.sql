select location, budget
from departments
where dept_name in ('Legal', 'Sales', 'HR') or budget <= 2415000
order by budget, location;
