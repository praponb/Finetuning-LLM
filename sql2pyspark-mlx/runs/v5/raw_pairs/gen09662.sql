select id, cost
from shipments
where cost >= (select max(cost) from shipments);
