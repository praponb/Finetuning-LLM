with ranked as (
    select *, row_number() over (partition by location order by budget desc, id) as rn
    from departments
    where budget >= 1745000
)
select
    id,
    location,
    budget,
    dept_name
from ranked
where rn <= 5
