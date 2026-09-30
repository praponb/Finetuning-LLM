select status
from shipments
union
select status
from subscriptions
order by status
