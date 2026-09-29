select
    id,
    shipped_date,
    cost,
    order_id,
    case when carrier in ('Kerry', 'FedEx') then 'Priority' else 'Other' end as carrier_group
from shipments
where order_id = 31 and (order_id is not null or delivered_date between '2023-07-01' and '2024-09-01')
order by order_id desc;
