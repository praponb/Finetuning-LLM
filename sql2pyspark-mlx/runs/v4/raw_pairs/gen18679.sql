select *
from (
    select *, row_number() over (partition by event_type order by duration_sec desc, id asc) as row_num
    from web_events
) ranked
where row_num = 1;
