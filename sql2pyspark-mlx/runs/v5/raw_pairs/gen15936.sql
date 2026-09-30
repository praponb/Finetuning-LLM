select id, location, budget
from (
    select
        id,
        location,
        budget,
        row_number() over (partition by location order by budget, id) as rnk
    from departments
) as t
where rnk <= 2
order by budget desc;
