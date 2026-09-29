select event_type, sum(duration_sec) as total_duration_sec, dense_rank() over (order by sum(duration_sec) desc) as rnk from web_events group by event_type order by total_duration_sec desc, rnk desc
