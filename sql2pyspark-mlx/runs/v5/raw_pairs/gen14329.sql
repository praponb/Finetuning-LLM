select session_id from web_events where duration_sec between 331 and 336 union select session_id from web_events where session_id in (26, 36, 40) order by session_id desc;
