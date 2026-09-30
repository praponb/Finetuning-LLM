with with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by device order by started_at asc, id asc) is null
                 or started_at > lag(started_at) over (partition by device order by started_at asc, id asc) + interval 30 days then 1 else 0 end as session_start_flag
    from sessions
)
-- Running sum of the flag = session number
select *, sum(session_start_flag) over (partition by device order by started_at asc, id asc rows between unbounded preceding and current row) as burst_id
from with_gaps
