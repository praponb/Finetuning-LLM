select status, count(cost) as cost_count
from shipments
group by status;
