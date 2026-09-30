select * from (select page, event_type, duration_sec from web_events where event_type = 'purchase')
pivot (sum(duration_sec) for event_type in ('add_to_cart', 'page_view', 'purchase', 'search'))
order by page
