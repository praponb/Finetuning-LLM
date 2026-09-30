select
    id,
    location,
    budget,
    lower(dept_name) dept_name_lower
from departments
where location = 'New York'
order by dept_name_lower
