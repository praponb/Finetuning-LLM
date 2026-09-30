select id, device, started_at
from (
    select *, row_number() over (partition by device order by started_at, id) as rn
    from sessions
    where hour(started_at) between 8 and 16
) as t
where rn <= 1;
