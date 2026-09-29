select status
from orders
where customer_id is not null
union all
select status
from shipments
where cost between 27 and 93
