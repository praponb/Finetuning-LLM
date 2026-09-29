select s.id, s.browser from sessions s where s.id in (select session_id from web_events where to_date(event_time) < '2024-01-01') order by browser
