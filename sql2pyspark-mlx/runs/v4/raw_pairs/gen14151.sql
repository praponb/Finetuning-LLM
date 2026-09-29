with numbered as (select *, row_number() over (partition by event_type order by duration_sec desc, id) as rn from web_events) select * from numbered where rn <= 3;
