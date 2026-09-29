select distinct page, event_type
from web_events
order by page, event_type desc
