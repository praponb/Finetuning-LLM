select
    id,
    dept_name,
    location,
    budget
from departments
order by location desc, id asc
limit 5;
