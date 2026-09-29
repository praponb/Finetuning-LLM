select
    a.id,
    a.session_id,
    a.page,
    a.event_time,
    a.duration_sec,
    b.country,
    b.browser,
    b.device
from web_events a
left join sessions b
    on a.session_id = b.id
