select
    budget,
    dept_name,
    location,
    length(location) as location_length
from departments
where budget > 2291000
order by budget, location_length desc
