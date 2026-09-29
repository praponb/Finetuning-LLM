select * from (select *, row_number() over (partition by page order by event_time desc, id) as rn from web_events) as ranked where rn <= 3 order by id desc;
