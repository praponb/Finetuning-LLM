select
    id,
    browser,
    started_at,
    device
from (
    select *, row_number() over (partition by browser order by started_at, id) as rn
    from sessions
) as r
where rn <= 3
order by device
