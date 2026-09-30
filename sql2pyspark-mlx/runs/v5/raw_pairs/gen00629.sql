with session_stats as (
    select session_id, round(stddev(duration_sec), 2) as stddev_duration_sec
    from web_events
    where duration_sec > 227
    group by session_id
)
select s.device, s.browser, ss.stddev_duration_sec
from sessions s
inner join session_stats ss on s.id = ss.session_id
order by device
