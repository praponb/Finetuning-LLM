select
    id,
    event_time,
    case when duration_sec >= 470 then 'High' when duration_sec >= 122 then 'Medium' else 'Low' end duration_sec_level,
    coalesce(page, 'unassigned') page_filled
from web_events
where event_type = 'click' or session_id in (2, 15, 31);
