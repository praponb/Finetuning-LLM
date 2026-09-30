select
    id,
    device,
    started_at,
    ntile(2) over (partition by device order by started_at, id) as bucket
from sessions
where hour(started_at) between 8 and 16;
