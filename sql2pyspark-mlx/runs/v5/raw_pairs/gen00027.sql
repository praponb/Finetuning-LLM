select
    p.city,
    p.region,
    a.status,
    count(*) as cnt,
    avg(a.shipping_fee) as avg_shipping_fee,
    max(a.amount) as max_amount
from orders a
inner join stores p on a.store_id = p.id
group by grouping sets ((p.city, p.region, a.status), (p.city, p.region), (p.city), ());
