select id, event_type, page, floor(duration_sec) duration_sec_whole, ceil(duration_sec) duration_sec_whole_2 from web_events where session_id = 2 order by page, duration_sec_whole_2 desc;
