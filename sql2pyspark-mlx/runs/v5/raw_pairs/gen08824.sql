select web_events.event_time, web_events.duration_sec, sessions.device from web_events inner join sessions on web_events.session_id = sessions.id order by web_events.event_time desc;
