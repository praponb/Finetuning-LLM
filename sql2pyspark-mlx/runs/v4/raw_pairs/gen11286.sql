select id, page, event_time, rank() over (partition by page order by event_time) as event_time_rank from web_events;
