select id, status, order_id
from shipments
where order_id in (2, 16, 36, 38)
union
select id, status, order_id
from shipments
where cost > 66;
