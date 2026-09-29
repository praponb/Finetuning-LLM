select *
from (
    select *, row_number() over (partition by city order by sqft desc, id) as pos
    from stores
) as t
where pos <= 2
