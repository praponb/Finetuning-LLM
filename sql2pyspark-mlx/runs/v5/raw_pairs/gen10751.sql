select launch_date as event_date
from products
where brand <> 'Acme'
union all
select delivered_date as event_date
from shipments
where carrier in ('Kerry', 'Flash', 'FedEx')
order by event_date;
