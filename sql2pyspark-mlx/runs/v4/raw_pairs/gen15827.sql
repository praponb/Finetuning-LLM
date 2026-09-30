select id from web_events except distinct select id from web_events where duration_sec > 470
