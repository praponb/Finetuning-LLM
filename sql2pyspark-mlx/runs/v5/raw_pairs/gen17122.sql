select * from (select id, event_type, duration_sec, row_number() over (partition by event_type order by duration_sec desc, id) row_num from web_events) r where row_num <= 5;
