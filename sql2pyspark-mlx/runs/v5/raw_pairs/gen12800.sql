select
    page,
    event_time,
    event_type,
    date_format(event_time, 'MMM yyyy') as event_time_label,
    coalesce(duration_sec, 0) as duration_sec_filled
from web_events
where event_type = 'page_view' and page = '/cart'
