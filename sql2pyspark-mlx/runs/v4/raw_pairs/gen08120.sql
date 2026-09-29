select we.id, we.event_type, we.event_time, s.country, s.browser from web_events we inner join sessions s on we.session_id = s.id
