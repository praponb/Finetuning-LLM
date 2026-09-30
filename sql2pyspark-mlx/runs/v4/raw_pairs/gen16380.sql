with metrics as (
    select
        *,
        sum(duration_sec) over (partition by page order by event_time, id rows between unbounded preceding and current row) as cumulative_duration_sec,
        stddev_samp(duration_sec) over w_recent as event_type_stddev_samp_duration_sec,
        percent_rank() over w_recent_2 as duration_sec_percent_rank,
        avg(duration_sec) over w_recent as event_type_avg_duration_sec,
        avg(duration_sec) over (partition by session_id order by cast(event_time as bigint) range between 604800 preceding and current row) as duration_sec_avg_7d
    from web_events
    window w_recent as (partition by event_type), w_recent_2 as (partition by session_id order by duration_sec desc nulls last)
)
select
    s.id,
    s.session_id,
    s.duration_sec,
    s.cumulative_duration_sec,
    s.event_type_stddev_samp_duration_sec,
    s.duration_sec_percent_rank,
    (select max(w2.duration_sec) from web_events w2 where w2.session_id = s.session_id) as max_web_event_duration_sec
from metrics s
order by max_web_event_duration_sec desc, cumulative_duration_sec
