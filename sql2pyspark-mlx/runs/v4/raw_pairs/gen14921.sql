select id, order_id, status
from shipments
where order_id is null
order by id;
