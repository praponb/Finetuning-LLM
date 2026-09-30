select start_date, 'subscription' as source
from subscriptions
union all
select shipped_date as start_date, 'shipment' as source
from shipments
