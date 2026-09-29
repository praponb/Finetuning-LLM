select start_date
from subscriptions
where status in ('PAUSED', 'ACTIVE', 'CANCELLED')
union
select shipped_date
from shipments
