select *
from (
    select page, avg(duration_sec) as avg_duration_sec
    from web_events
    group by page
) as agg
where avg_duration_sec < 220
order by avg_duration_sec desc;
