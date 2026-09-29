select order_date, 'order' as source
from orders
union
select shipped_date, 'shipment' as source
from shipments
