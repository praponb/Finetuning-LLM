select s.device, s.customer_id, we.event_type
from sessions s
inner join web_events we on we.session_id = s.id
