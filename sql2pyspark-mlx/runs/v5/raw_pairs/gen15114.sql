select
    id,
    session_id,
    duration_sec,
    row_number() over (partition by session_id order by duration_sec, id) rn
from web_events
where event_type = 'search';
