select id, event_type, event_time, duration_sec, case when event_type = 'page_view' then 1 else 0 end as is_page_view from web_events where event_type = 'add_to_cart' order by duration_sec desc
