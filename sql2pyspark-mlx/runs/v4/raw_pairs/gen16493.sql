select launch_date as event_date, 'product' as source
from products
union all
select shipped_date as event_date, 'shipment' as source
from shipments;
