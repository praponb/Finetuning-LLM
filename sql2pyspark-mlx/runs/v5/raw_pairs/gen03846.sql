select status
from shipments
where carrier is not null
intersect
select status
from subscriptions
