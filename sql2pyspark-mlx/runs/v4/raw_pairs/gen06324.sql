select
    id,
    page,
    event_type,
    session_id
from web_events
where event_time >= '2024-08-04';
