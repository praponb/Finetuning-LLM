SELECT s.* FROM sessions s WHERE EXISTS (SELECT 1 FROM web_events ch WHERE ch.session_id = s.id AND ch.event_time >= s.started_at)
