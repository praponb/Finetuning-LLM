select status
from shipments
where carrier is not null and carrier in ('UPS', 'FedEx') and cost is null
order by status asc;
