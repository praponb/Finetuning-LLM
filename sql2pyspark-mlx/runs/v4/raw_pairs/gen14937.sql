select *
from (
    select
        id,
        dept_name,
        budget,
        location,
        rank() over (partition by dept_name order by budget desc) as pos
    from departments
) as t
where pos = 1
