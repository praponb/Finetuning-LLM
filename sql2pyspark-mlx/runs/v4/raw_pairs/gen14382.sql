select
    id,
    location,
    budget,
    row_number() over (partition by location order by budget, id) as rn
from departments
where budget between 3345000 and 3569000
