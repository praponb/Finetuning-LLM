with base as (
    select
        *,
        length(location) location_length,
        coalesce(dept_name, 'NONE') resolved_dept_name
    from departments
    where dept_name <> 'Legal'
)
select
    id,
    dept_name,
    location,
    location_length
from base
qualify row_number() over (partition by dept_name order by budget asc, id asc) = 1;
