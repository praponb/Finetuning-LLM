select
    id,
    location,
    budget,
    rank() over (partition by location order by budget) as rnk
from departments
where budget between 1953000 and 3520000;
