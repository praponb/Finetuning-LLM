with gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(started_at) over (partition by customer_id order by started_at, id) is null
                 or started_at > lag(started_at) over (partition by customer_id order by started_at, id) + interval '14' day then 1 else 0 end as new_group_flag
    from sessions
)
-- Running sum of the flag = session number
select
    id,
    customer_id,
    device,
    browser,
    started_at,
    sum(new_group_flag) over (partition by customer_id order by started_at, id rows between unbounded preceding and current row) as burst_id
from gaps
