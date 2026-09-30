select duration_sec, event_type, concat_ws('/', event_type, page) as event_type_page_key from web_events order by event_type_page_key;
