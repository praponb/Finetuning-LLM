select s.device, max(we.event_time) as last_event_time from web_events we inner join sessions s on we.session_id = s.id where s.country = 'JP' group by s.device order by device desc
