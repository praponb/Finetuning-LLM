with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        event_type,
        page,
        event_time,
        case when duration_sec >= 435 then 'High' when duration_sec >= 223 then 'Medium' else 'Low' end as duration_sec_level
    from web_events
    where session_id in (34, 40)
),
latest as (
    -- QUALIFY-style window filter
    select *
    from base
    qualify row_number() over (partition by page, event_type order by event_time desc, id) = 1
),
gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when event_time - lag(event_time) over (partition by page order by event_time, id) > interval '90' day then 1 else 0 end as session_start_flag
    from latest
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        page,
        event_time,
        sum(session_start_flag) over (partition by page order by event_time, id rows between unbounded preceding and current row) as burst_id
    from gaps
),
base_web_events as (
    -- Keep recent, valid rows
    select
        *,
        cast(event_time as date) as event_time_day,
        hour(event_time) as event_time_hour
    from islands
    where page is not null
      and event_time > timestamp '2023-03-11 09:00:00'
)
select
    id,
    page,
    event_time,
    burst_id,
    event_time_day,
    event_time_hour
from base_web_events
order by event_time_hour nulls last;
