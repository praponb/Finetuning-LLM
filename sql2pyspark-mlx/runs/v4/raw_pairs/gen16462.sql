select id, session_id, concat(page, '/', event_type) page_event_type
from web_events
where hour(event_time) between 9 and 12 and event_time < '2023-10-01'
