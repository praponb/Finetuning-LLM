select max(duration_sec) - min(duration_sec) as duration_sec_range
from web_events
where (duration_sec != 452 and duration_sec = 127) or duration_sec between 130 and 143
