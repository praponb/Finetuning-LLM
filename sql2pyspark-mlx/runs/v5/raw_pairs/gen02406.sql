select
    id,
    session_id,
    event_type,
    duration_sec,
    concat_ws('-', event_type, page) as event_type_page_key
from web_events
where session_id in (3, 30, 39) or event_time < '2023-12-01'
