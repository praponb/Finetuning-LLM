with with_gaps as (
    select *, case when lag(started_at) over (partition by browser order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by browser order by started_at, id) + interval '90' day then 1 else 0 end as is_new_burst
    from sessions
)
select
    id,
    device,
    browser,
    started_at,
    country,
    sum(is_new_burst) over (partition by browser order by started_at, id rows between unbounded preceding and current row) as session_no
from with_gaps
