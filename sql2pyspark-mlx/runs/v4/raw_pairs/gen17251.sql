with flagged as (
    select *, case when lag(event_time) over (partition by event_type order by event_time, id) is null
                 or event_time > lag(event_time) over (partition by event_type order by event_time, id) + interval 30 days then 1 else 0 end as session_start_flag
    from web_events
),
islands as (
    select *, sum(session_start_flag) over (partition by event_type order by event_time, id rows between unbounded preceding and current row) as session_id_2
    from flagged
)
select
    case when session_id_2 >= 7 then 'PREMIUM' else 'STANDARD' end as session_id_2_tier,
    (unix_timestamp(max(event_time)) - unix_timestamp(min(event_time))) / 60 as span_minutes,
    max(event_time) as last_seen,
    max(event_time) as last_event_time,
    grouping(case when session_id_2 >= 7 then 'PREMIUM' else 'STANDARD' end) as gid
from islands
group by rollup (case when session_id_2 >= 7 then 'PREMIUM' else 'STANDARD' end);
