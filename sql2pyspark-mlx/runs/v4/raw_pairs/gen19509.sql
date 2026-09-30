select
    s.id,
    s.shipped_date,
    s.status,
    s.carrier
from shipments s
where s.order_id in (select id from orders where order_date between '2023-05-02' and '2024-06-01');
