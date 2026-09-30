select * from (select *, row_number() over (partition by event_type, page order by duration_sec desc, id) as rn from web_events) x where rn = 1
