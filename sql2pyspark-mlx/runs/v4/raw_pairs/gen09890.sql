select status, 'order' as source
from orders
union
select status, 'shipment' as source
from shipments
order by status;
