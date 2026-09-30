with flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when started_at - lag(started_at) over (partition by device order by started_at, id) > interval 60 days then 1 else 0 end as new_group_flag
    from sessions
)
-- Running sum of the flag = session number
select *, sum(new_group_flag) over (partition by device order by started_at, id rows between unbounded preceding and current row) as burst_id
from flagged
