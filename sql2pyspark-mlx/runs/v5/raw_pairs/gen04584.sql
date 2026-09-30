select
    duration_sec,
    page,
    session_id,
    initcap(trim(page)) page_title
from web_events
where hour(event_time) between 11 and 19 or page = '/product'
order by page_title
