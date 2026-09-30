with prev_events as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by status order by paid_at, id) as prev_paid_at
    from payments
),
gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '7' day then 1 else 0 end as new_group_flag
    from prev_events
)
-- Running sum of the flag = session number
select
    id,
    method,
    paid_at,
    status,
    sum(new_group_flag) over (partition by status order by paid_at, id rows between unbounded preceding and current row) as session_id
from gaps;
