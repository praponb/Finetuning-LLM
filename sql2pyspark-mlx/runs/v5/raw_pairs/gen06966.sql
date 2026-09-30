with valid as (
    select
        *,
        lower(location) as location_lower,
        concat(dept_name, ' - ', location) as dept_name_location
    from departments
    where dept_name is distinct from 'Engineering'
)
select *
from (
    select
        dept_name,
        location,
        budget
    from valid
) src
pivot (
    max(budget) as amount,
    count(1) as n
    for location in ('Singapore' as singapore, 'New York' as new_york)
)
