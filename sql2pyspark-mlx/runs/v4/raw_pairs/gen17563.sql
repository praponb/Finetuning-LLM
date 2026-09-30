with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        hour(event_time) as event_time_hour
    from web_events
    where page is not null
),
latest as (
    -- QUALIFY-style window filter
    select *
    from (
        select *, row_number() over (partition by event_type order by event_time nulls first, id) as latest_rank
        from filtered
    ) t
    where latest_rank = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        event_time_hour,
        latest_rank,
        row_number() over (order by latest_rank desc, id) as seq_no,
        rank() over (partition by event_type order by duration_sec nulls last) as duration_sec_rank,
        avg(event_time_hour) over (partition by session_id, event_type) as session_id_avg_event_time_hour
    from latest
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval '60' day then 1 else 0 end as is_new_burst
    from enriched
),
islands as (
    -- Running sum of the flag = session number
    select *, sum(is_new_burst) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as burst_id
    from flagged
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        session_id,
        event_type,
        event_time,
        duration_sec,
        latest_rank,
        seq_no,
        duration_sec_rank,
        burst_id,
        case when event_type in ('add_to_cart', 'click') then 'Core' else 'Other' end as event_type_group
    from islands
    where event_time >= timestamp '2023-12-13 08:00:00'
      and event_type != 'search'
)
-- Grouped metrics
select
    date_trunc('week', event_time) as week_start,
    date_trunc('month', event_time) as activity_month,
    min(event_time) as started_at,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as span_minutes
from base
group by cube (date_trunc('week', event_time), date_trunc('month', event_time))
having (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 < 0
