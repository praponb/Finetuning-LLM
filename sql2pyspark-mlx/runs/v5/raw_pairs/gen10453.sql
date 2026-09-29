with with_prev as (
    -- Previous event time per key
    select *, lag(event_time) over (partition by event_type order by event_time, id) as prev_event_time
    from web_events
),
with_gaps as (
    -- Flag rows that start a new session
    select *, case when prev_event_time is null or event_time > prev_event_time + interval 14 days then 1 else 0 end as is_new_session
    from with_prev
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        sum(is_new_session) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
-- Multi-level aggregation
select
    date_trunc('quarter', event_time) as quarter_start,
    bit_or(duration_sec) as duration_sec_bit_or,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as duration_min,
    count_if(event_type = 'add_to_cart') as add_to_cart_events,
    size(collect_set(event_type)) as num_event_types,
    size(collect_set(event_type)) as num_event_types_2,
    bit_or(duration_sec) as duration_sec_bit_or_2
from islands
group by date_trunc('quarter', event_time);
