select
    page,
    event_type,
    event_time,
    concat(event_type, '/', page) as event_type_page
from web_events
where to_date(event_time) >= '2023-07-01'
