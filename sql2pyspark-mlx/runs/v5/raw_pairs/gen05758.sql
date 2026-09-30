with prev_events as (
    -- Previous event time per key
    select *, lag(paid_at) over (partition by method order by paid_at, id) as prev_paid_at
    from payments
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_paid_at is null or paid_at > prev_paid_at + interval '14' day then 1 else 0 end as is_new_session
    from prev_events
)
-- Running sum of the flag = session number
select
    id,
    order_id,
    method,
    paid_at,
    sum(is_new_session) over (partition by method order by paid_at, id rows between unbounded preceding and current row) as session_id
from with_gaps;
