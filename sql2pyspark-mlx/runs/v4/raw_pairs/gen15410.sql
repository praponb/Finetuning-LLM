select id, event_type, session_id from web_events where page not in ('/home', '/checkout') union select id, event_type, session_id from web_events where event_time < '2024-07-01' order by session_id
