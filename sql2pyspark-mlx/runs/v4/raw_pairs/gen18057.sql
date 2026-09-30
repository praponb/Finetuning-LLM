select we.duration_sec, s.browser, s.country
from web_events we
inner join sessions s on s.id = we.session_id
