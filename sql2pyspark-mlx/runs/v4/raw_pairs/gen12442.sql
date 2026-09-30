select we.id, we.duration_sec, we.event_type
from web_events we
where we.session_id in (select id from sessions where customer_id in (6, 11, 39));
