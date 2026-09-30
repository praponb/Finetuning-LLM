select min(event_time) as first_event_time from web_events order by first_event_time desc;
