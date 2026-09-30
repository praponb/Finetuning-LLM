select * from (select session_id, page, event_type, duration_sec from web_events) src pivot (avg(duration_sec) as total, count(1) as item_count for event_type in ('page_view', 'click'))
