with RankedSessions as (
    select *, row_number() over (partition by device order by started_at desc, id) as rnk
    from sessions
)
select *
from RankedSessions
where rnk = 1
order by device
