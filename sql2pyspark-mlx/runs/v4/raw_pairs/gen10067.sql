select *
from (
    select case when duration_sec >= 380 then 'high' when duration_sec >= 127 then 'mid' else 'low' end as duration_sec_bucket, avg(duration_sec) as avg_duration_sec
    from web_events
    group by case when duration_sec >= 380 then 'high' when duration_sec >= 127 then 'mid' else 'low' end
) as agg
where avg_duration_sec > 340
order by avg_duration_sec asc;
