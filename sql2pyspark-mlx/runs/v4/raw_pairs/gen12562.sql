select
    s.id,
    s.cost,
    s.status,
    s.shipped_date
from shipments s
where s.carrier is not null and s.order_id in (select id from orders where order_date between '2023-11-23' and '2024-05-02')
