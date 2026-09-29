select *, upper(event_type) as event_type_upper, concat(page, '/', event_type) as page_event_type from web_events where session_id in (7, 13, 21) and to_date(event_time) < '2023-09-01'
