select 'store' as record_type, opened_date
from stores
where region in ('North', 'Central')
union
select 'shipment' as record_type, delivered_date
from shipments
where cost between 37 and 48;
