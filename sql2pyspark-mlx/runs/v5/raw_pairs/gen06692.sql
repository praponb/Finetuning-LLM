select status
from payments
union all
select status
from shipments
where cost <= 95
