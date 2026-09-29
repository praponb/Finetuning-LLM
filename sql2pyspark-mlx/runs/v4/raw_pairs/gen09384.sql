select device, to_date(started_at) as started_at_date
from sessions
order by device;
