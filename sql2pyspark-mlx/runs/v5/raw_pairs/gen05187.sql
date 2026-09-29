select case when duration_sec >= 459 then 'high' when duration_sec >= 212 then 'mid' else 'low' end as duration_sec_bucket, round(avg(duration_sec), 2) as avg_duration_sec
from web_events
group by case when duration_sec >= 459 then 'high' when duration_sec >= 212 then 'mid' else 'low' end
having avg_duration_sec < 59
order by avg_duration_sec, duration_sec_bucket desc;
