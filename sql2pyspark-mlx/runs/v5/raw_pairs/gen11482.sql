select *, floor(cost) as cost_whole
from shipments
where status in ('DELIVERED', 'LOST', 'IN_TRANSIT')
order by status, id
limit 10
