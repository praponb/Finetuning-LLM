with ranked as (
    select
        id,
        dept_name,
        budget,
        location,
        row_number() over (partition by dept_name order by budget desc, id) as rnk
    from departments
)
select
    id,
    dept_name,
    budget,
    location
from ranked
where rnk <= 3
order by budget desc, id desc
