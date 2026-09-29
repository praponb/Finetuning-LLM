select
    o.id,
    o.discount,
    o.order_date,
    o.channel
from orders o
where o.store_id not in (select id from stores where city in ('Bangkok', 'New York'))
order by discount, o.order_date;
