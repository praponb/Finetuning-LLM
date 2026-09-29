select status
from shipments
where order_id = 12
union
select status
from shipments
where carrier = 'UPS';
