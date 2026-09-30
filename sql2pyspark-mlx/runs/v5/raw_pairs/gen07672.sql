select delivered_date
from shipments
except
select launch_date
from products
order by delivered_date
