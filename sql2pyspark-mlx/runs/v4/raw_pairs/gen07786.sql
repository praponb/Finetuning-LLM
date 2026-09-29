select status, shipped_date
from shipments
where status <> 'LOST'
union
select status, shipped_date
from shipments
where order_id is not null
