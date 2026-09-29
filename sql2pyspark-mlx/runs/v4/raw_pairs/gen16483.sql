select status, 'order' source
from orders
union all
select status, 'shipment' source
from shipments
where delivered_date > '2024-06-01'
order by status desc;
