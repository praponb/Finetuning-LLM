select
    id,
    dept_name,
    budget,
    location
from (
    select *, row_number() over (partition by dept_name order by budget desc, id) as rn
    from departments
    where budget <= 3119000
) as r
where rn <= 3
order by location desc, id desc
