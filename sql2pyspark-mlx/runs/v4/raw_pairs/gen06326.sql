with TopSessions as (
    select
        id,
        browser,
        started_at,
        row_number() over (partition by browser order by started_at desc, id) as pos
    from sessions
)
select *
from TopSessions
where pos = 1
order by browser desc
