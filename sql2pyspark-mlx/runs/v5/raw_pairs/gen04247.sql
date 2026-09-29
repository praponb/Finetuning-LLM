select id, event_time, ntile(3) over (order by event_time, id) as quartile from web_events where hour(event_time) between 9 and 16 order by 2 desc
