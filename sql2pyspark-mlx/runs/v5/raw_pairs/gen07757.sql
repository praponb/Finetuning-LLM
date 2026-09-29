select status
from orders
except
select status
from shipments
where delivered_date between '2023-03-01' and '2025-10-12';
